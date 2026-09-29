from pyspark.sql import functions as F

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.voltage").alias("prior_voltage_7d"),
    )
)

site_id_stats = (
    matched
    .groupBy("site_id")
    .agg(F.max("prior_voltage_7d").alias("max_prior_voltage_7d"))
)

vs_group = (
    matched.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_voltage_7d") == F.col("g.max_prior_voltage_7d"))
    .select(
        "s.*",
        "g.max_prior_voltage_7d",
        F.round(F.col("s.prior_voltage_7d") - F.col("g.max_prior_voltage_7d"), 2).alias("prior_voltage_7d_minus_max"),
    )
)

result = (
    vs_group
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_7d",
        "prior_voltage_7d",
        "max_prior_voltage_7d",
        "prior_voltage_7d_minus_max",
        F.round(F.col("prior_voltage_7d") / F.nullif(F.col("max_prior_voltage_7d"), F.lit(0)), 3).alias("prior_voltage_7d_per_max_prior_voltage_7d"),
    )
)
