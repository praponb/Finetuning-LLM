from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("site_id") >= 5) & (F.col("kwh") >= 46.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

per_group = (
    scoped
    .filter(F.col("voltage") >= 222.1)
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
    )
)

labeled = (
    per_group
    .select(
        "*",
        F.abs(F.col("row_count") - 6).alias("row_count_distance"),
        (F.floor(F.col("n_quality_is_not_null") / 2) * 2).alias("n_quality_is_not_null_bucket"),
        F.coalesce(F.col("n_quality_is_not_null"), F.lit(0)).alias("n_quality_is_not_null_filled"),
    )
)

with_activity = (
    labeled
    .crossJoin(labeled.agg(F.avg("n_quality_is_not_null").alias("overall_avg_n_quality_is_not_null")))
    .filter(F.col("n_quality_is_not_null") > F.col("overall_avg_n_quality_is_not_null"))
    .drop("overall_avg_n_quality_is_not_null")
)

filtered = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("n_quality_is_not_null_filled").alias("overall_avg_n_quality_is_not_null_filled")))
    .filter(F.col("n_quality_is_not_null_filled") > F.col("overall_avg_n_quality_is_not_null_filled"))
    .drop("overall_avg_n_quality_is_not_null_filled")
)

scored = (
    filtered
    .select(
        "*",
        F.date_format(F.col("read_at_month"), "MMM yyyy").alias("read_at_month_label"),
        F.date_trunc("week", F.col("read_at_month")).alias("read_at_month_week"),
    )
)

result = (
    scored
    .rollup("row_count_distance", "n_quality_is_not_null_filled")
    .agg(
        F.sum("n_quality_is_not_null").alias("total_n_quality_is_not_null"),
        F.count("*").alias("row_count_2"),
        F.min("read_at_month").alias("first_read_at_month"),
    )
    .orderBy(
        F.col("row_count_distance").asc_nulls_last(),
        F.col("n_quality_is_not_null_filled").asc_nulls_last(),
    )
)
