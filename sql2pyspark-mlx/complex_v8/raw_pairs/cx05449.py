from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.kwh").alias("prior_kwh_12h"),
    )
)

per_group = (
    joined_ranges
    .groupBy("prior_rows_12h", "site_id")
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.percentile_approx("prior_kwh_12h", 0.5).alias("median_prior_kwh_12h"),
        F.sum("prior_kwh_12h").alias("total_prior_kwh_12h"),
        F.min("prior_rows_12h").alias("min_prior_rows_12h"),
        F.percentile_approx("prior_rows_12h", 0.5).alias("median_prior_rows_12h"),
    )
)

result = (
    per_group
    .select(
        "prior_rows_12h",
        "site_id",
        "distinct_site_ids",
        "median_prior_kwh_12h",
        "total_prior_kwh_12h",
        "min_prior_rows_12h",
        "median_prior_rows_12h",
        F.least(F.col("prior_rows_12h"), F.col("total_prior_kwh_12h")).alias("least_prior_rows_12h_total_prior_kwh_12h"),
    )
    .orderBy(F.col("min_prior_rows_12h").asc_nulls_last(), "median_prior_kwh_12h")
)
