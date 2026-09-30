from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_by_start_dat = Window.partitionBy(F.year("start_date")).orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_start_date_year_by_start_dat_2 = Window.partitionBy(F.year("start_date"))
w_start_date_year_by_start_dat_3 = Window.partitionBy(F.year("start_date")).orderBy(F.col("start_date").asc_nulls_last(), "id")
w_start_date_year_by_days_star = Window.partitionBy(F.year("start_date")).orderBy(F.col("days_start_date_to_end_date").desc(), "id")

recent = (
    df_policies
    .filter((F.col("premium") > 2289.0) & (F.col("product") != "AUTO"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

tagged = (
    recent
    .select(
        "id",
        "premium",
        "start_date",
        "product_slug",
        "days_start_date_to_end_date",
        F.substring(F.trim(F.col("product_slug")), 1, 3).alias("product_slug_prefix"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.round(F.avg("days_start_date_to_end_date").over(w_start_date_year_by_start_dat), 2).alias("days_start_date_to_end_date_avg_3_prior"),
        F.round(F.avg("days_start_date_to_end_date").over(w_start_date_year_by_start_dat_2), 2).alias("start_date_year_avg_days_start_date_to_end_date"),
        F.dense_rank().over(w_start_date_year_by_start_dat_3).alias("start_date_dense_rank"),
        F.lead("start_date").over(w_start_date_year_by_days_star).alias("next_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_start_date_year_by_start_dat_2), F.lit(0)), 4).alias("premium_share_of_start_date_year"),
    )
)

grouped = (
    with_trends
    .groupBy(F.year("next_start_date").alias("next_start_date_year"))
    .agg(
        F.max("next_start_date").alias("last_next_start_date"),
        F.round(F.stddev("start_date_dense_rank"), 2).alias("stddev_start_date_dense_rank"),
        F.max("start_date").alias("last_start_date"),
        F.max("start_date_dense_rank").alias("max_start_date_dense_rank_check"),
    )
    .filter(F.col("max_start_date_dense_rank_check") > 3)
    .drop("max_start_date_dense_rank_check")
)

annotated = (
    grouped
    .withColumn("last_next_start_date_label", F.date_format(F.col("last_next_start_date"), "yyyy-MM-dd"))
)

labeled = (
    annotated
    .filter(F.col("next_start_date_year") < 2025)
    .select(
        "next_start_date_year",
        "last_next_start_date",
        "stddev_start_date_dense_rank",
        "last_next_start_date_label",
        F.date_format(F.col("last_start_date"), "yyyy-MM-dd").alias("last_start_date_label"),
        F.abs(F.col("next_start_date_year") - 2025).alias("next_start_date_year_distance"),
    )
)

result = (
    labeled
    .select(
        "next_start_date_year",
        "last_next_start_date",
        "stddev_start_date_dense_rank",
        "last_next_start_date_label",
        "next_start_date_year_distance",
        F.add_months(F.col("last_next_start_date"), 6).alias("last_next_start_date_plus_6m"),
        F.date_trunc("quarter", F.col("last_next_start_date")).alias("last_next_start_date_quarter"),
    )
)
