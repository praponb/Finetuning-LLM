from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy(F.col("premium_distance").desc(), F.col("id").desc())

recent_policys = (
    df_policies
    .filter((F.year(F.col("end_date")) == 2025) & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
        F.when(F.col("premium") >= 3465.4, "HIGH").when(F.col("premium") >= 1132.1, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

unique_rows = (
    recent_policys
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

tagged = (
    unique_rows
    .filter(F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-03-24").cast("date")))
    .select(
        "*",
        F.least(F.col("premium_distance"), F.col("days_since_start_date")).alias("least_premium_distance_days_since_start_date"),
    )
)

per_group = (
    tagged
    .groupBy(
        "product",
        "status",
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
    )
    .agg(
        F.percentile_approx("premium_distance", 0.5).alias("median_premium_distance"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.min("least_premium_distance_days_since_start_date").alias("min_least_premium_distance_days_since_start_date"),
    )
)

labeled = (
    per_group
    .select(
        "product",
        "status",
        "end_date_ym",
        "median_premium_distance",
        "distinct_holder_ids",
        "status_list",
        F.round(F.col("median_premium_distance") / F.nullif(F.col("distinct_holder_ids"), F.lit(0)), 3).alias("median_premium_distance_per_distinct_holder_ids"),
    )
)

result = (
    labeled
    .filter(F.col("distinct_holder_ids") >= 3)
    .select(
        "product",
        "status",
        "end_date_ym",
        "distinct_holder_ids",
        "status_list",
        "median_premium_distance_per_distinct_holder_ids",
    )
    .orderBy(
        F.col("product").desc_nulls_first(),
        F.col("end_date_ym").desc_nulls_first(),
    )
)
