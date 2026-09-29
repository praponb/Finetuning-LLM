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

filtered = (
    matched.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") < 373.3) & (F.upper(F.trim(F.col("si.site_name"))).like("H%")), "left_semi")
)

without_matches = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()) & (F.col("m.voltage") <= 240.1), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "read_at",
        "prior_rows_7d",
        "prior_voltage_7d",
        (F.floor(F.col("prior_voltage_7d") / 250) * 250).alias("prior_voltage_7d_bucket"),
        F.greatest(F.col("prior_rows_7d"), F.col("prior_voltage_7d")).alias("greatest_prior_rows_7d_prior_voltage_7d"),
    )
    .orderBy(F.col("read_at").desc_nulls_first())
)
