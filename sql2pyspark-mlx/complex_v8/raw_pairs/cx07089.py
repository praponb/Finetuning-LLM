from pyspark.sql import functions as F

covered = (
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

site_id_stats = (
    covered
    .groupBy("site_id")
    .agg(F.max("prior_rows_12h").alias("max_prior_rows_12h"))
)

compared = (
    covered.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_rows_12h") == F.col("g.max_prior_rows_12h"))
    .select(
        "s.*",
        "g.max_prior_rows_12h",
        F.round(F.col("s.prior_rows_12h") - F.col("g.max_prior_rows_12h"), 2).alias("prior_rows_12h_minus_max"),
    )
)

labeled = (
    compared
    .select(
        "*",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.round(F.col("prior_rows_12h") / F.nullif(F.col("prior_kwh_12h"), F.lit(0)), 3).alias("prior_rows_12h_per_prior_kwh_12h"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_kwh_12h",
        "max_prior_rows_12h",
        "prior_rows_12h_minus_max",
        "read_at_label",
        F.coalesce(F.col("prior_kwh_12h"), F.lit(0.0)).alias("prior_kwh_12h_filled"),
    )
    .orderBy("id", F.col("site_id").desc())
    .limit(5)
)
