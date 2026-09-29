from pyspark.sql import functions as F

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.voltage").alias("prior_voltage_3d"),
    )
)

screened = (
    matched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("prior_voltage_3d") >= 122.5)
)

result = (
    screened
    .groupingSets(
        [["prior_rows_3d", "site_id"], ["prior_rows_3d"], ["site_id"]],
        "prior_rows_3d", "site_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.percentile_approx("prior_rows_3d", 0.5).alias("median_prior_rows_3d"),
        F.max("read_at").alias("last_read_at"),
        F.round(F.sum("prior_rows_3d") / F.nullif(F.sum("prior_voltage_3d"), F.lit(0)), 3).alias("prior_rows_3d_to_prior_voltage_3d_ratio"),
    )
)
