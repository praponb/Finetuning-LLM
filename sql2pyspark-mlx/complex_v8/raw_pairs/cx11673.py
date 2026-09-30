from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_week_year_by_n_rows = Window.partitionBy("end_date_week_year").orderBy(F.col("n_rows").desc(), "end_date_week_year")
w_end_date_week_year_by_n_rows_2 = Window.partitionBy("end_date_week_year").orderBy(F.col("n_rows").desc())
w_end_date_week_year = Window.partitionBy("end_date_week_year")

recent_policys = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2026)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
        F.abs(F.col("premium") - 3532.7).alias("premium_distance"),
    )
)

rollup_base = (
    recent_policys
    .groupBy(F.year("end_date_week").alias("end_date_week_year"))
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.min("premium_distance").alias("min_premium_distance"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("min_premium_distance").alias("overall_avg_min_premium_distance")))
    .filter(F.col("min_premium_distance") > F.col("overall_avg_min_premium_distance"))
    .drop("overall_avg_min_premium_distance")
)

labeled = (
    screened
    .withColumn("n_rows_per_distinct_holder_ids", F.round(F.col("n_rows") / F.nullif(F.col("distinct_holder_ids"), F.lit(0)), 3))
)

derived = (
    labeled
    .select(
        "end_date_week_year",
        "n_rows",
        "distinct_holder_ids",
        "min_premium_distance",
        "product_list",
        "n_rows_per_distinct_holder_ids",
        F.round(F.col("min_premium_distance") * 0.25, 2).alias("min_premium_distance_adj"),
    )
)

metrics = (
    derived
    .select(
        "end_date_week_year",
        "n_rows",
        "distinct_holder_ids",
        "min_premium_distance_adj",
        F.row_number().over(w_end_date_week_year_by_n_rows).alias("seq_no"),
        F.round(F.percent_rank().over(w_end_date_week_year_by_n_rows_2), 3).alias("n_rows_percent_rank"),
        F.min("distinct_holder_ids").over(w_end_date_week_year).alias("end_date_week_year_min_distinct_holder_ids"),
        F.round(F.cume_dist().over(w_end_date_week_year_by_n_rows), 3).alias("n_rows_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "end_date_week_year",
        "n_rows",
        "min_premium_distance_adj",
        "seq_no",
        "n_rows_percent_rank",
        "end_date_week_year_min_distinct_holder_ids",
        "n_rows_cume_dist",
    )
    .orderBy(
        F.col("min_premium_distance_adj").desc(),
        F.col("n_rows_percent_rank").asc(),
    )
)
