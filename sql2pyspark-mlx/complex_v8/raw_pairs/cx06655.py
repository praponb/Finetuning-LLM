from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_n_product_life = Window.partitionBy("end_date_year", "n_product_life").orderBy(F.col("distinct_ids").desc())
w_distinct_ids_n_product_life = Window.partitionBy("distinct_ids", "n_product_life").orderBy(F.col("greatest_end_date_year_distinct_ids").desc(), "end_date_year")
w_all_by_distinct_ids_percent = Window.orderBy(F.col("distinct_ids_percent_rank").desc(), "end_date_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_product_life_moving = Window.orderBy(F.col("n_product_life").desc(), "end_date_year").rowsBetween(-1, Window.currentRow)
w_end_date_year = Window.partitionBy("end_date_year")
w_all_by_n_product_life_full = Window.orderBy(F.col("n_product_life").desc(), "end_date_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2026-02-26").cast("date")) | (F.col("product") == "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

summary = (
    prepared_policys
    .groupBy(F.year("end_date").alias("end_date_year"))
    .agg(
        F.sum(F.when(F.col("product") == "LIFE", 1).otherwise(0)).alias("n_product_life"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

tagged = (
    summary
    .filter(F.col("end_date_year") <= 2025)
    .select(
        "end_date_year",
        "n_product_life",
        "distinct_ids",
        (F.col("n_product_life") - F.col("distinct_ids")).alias("n_product_life_minus_distinct_ids"),
        F.greatest(F.col("end_date_year"), F.col("distinct_ids")).alias("greatest_end_date_year_distinct_ids"),
    )
)

sequenced = (
    tagged
    .select(
        "end_date_year",
        "n_product_life",
        "greatest_end_date_year_distinct_ids",
        F.round(F.percent_rank().over(w_end_date_year_n_product_life), 3).alias("distinct_ids_percent_rank"),
        (F.col("end_date_year") - F.lag("end_date_year").over(w_distinct_ids_n_product_life)).alias("end_date_year_change"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "end_date_year",
        "n_product_life",
        "greatest_end_date_year_distinct_ids",
        "distinct_ids_percent_rank",
        F.sum("distinct_ids_percent_rank").over(w_all_by_distinct_ids_percent).alias("running_distinct_ids_percent_rank"),
        F.sum("end_date_year_change").over(w_all_by_n_product_life_moving).alias("end_date_year_change_sum_2"),
        F.max("distinct_ids_percent_rank").over(w_end_date_year).alias("end_date_year_max_distinct_ids_percent_rank"),
        F.first("end_date_year_change").over(w_all_by_n_product_life_full).alias("first_end_date_year_change"),
        F.max("end_date_year").over(w_end_date_year).alias("end_date_year_max_end_date_year"),
        F.round(F.col("distinct_ids_percent_rank") - F.avg("distinct_ids_percent_rank").over(w_end_date_year), 2).alias("distinct_ids_percent_rank_vs_end_date_year_avg"),
    )
)

filtered = (
    sequenced_2
    .crossJoin(sequenced_2.agg(F.min("distinct_ids_percent_rank").alias("overall_min_distinct_ids_percent_rank")))
    .filter(F.col("distinct_ids_percent_rank") > F.col("overall_min_distinct_ids_percent_rank"))
    .drop("overall_min_distinct_ids_percent_rank")
)

with_activity = (
    filtered
    .crossJoin(filtered.agg(F.max("end_date_year_max_distinct_ids_percent_rank").alias("overall_max_end_date_year_max_distinct_ids_perce")))
    .filter(F.col("end_date_year_max_distinct_ids_percent_rank") < F.col("overall_max_end_date_year_max_distinct_ids_perce"))
    .drop("overall_max_end_date_year_max_distinct_ids_perce")
)

result = (
    with_activity
    .select(
        "end_date_year",
        "n_product_life",
        "greatest_end_date_year_distinct_ids",
        "distinct_ids_percent_rank",
        "running_distinct_ids_percent_rank",
        "end_date_year_change_sum_2",
        "end_date_year_max_distinct_ids_percent_rank",
        "end_date_year_max_end_date_year",
        "distinct_ids_percent_rank_vs_end_date_year_avg",
        F.round(F.col("n_product_life") / F.nullif(F.col("end_date_year"), F.lit(0)), 3).alias("n_product_life_per_end_date_year"),
        (F.col("n_product_life") + F.col("end_date_year")).alias("n_product_life_plus_end_date_year"),
    )
)
