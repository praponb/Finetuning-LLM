from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.when(F.col("voltage") >= 233.8, "HIGH").when(F.col("voltage") >= 222.3, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
    )
)

qualified = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")), "left_anti")
)

screened = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(6, 8)) & (F.col("m.quality").isNotNull()), "left_anti")
    .filter(F.col("voltage") >= 215.7)
)

result = (
    screened
    .rollup("site_id", "quality", "voltage_tier")
    .agg(F.round(F.avg("kwh"), 2).alias("avg_kwh"))
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("voltage_tier").asc_nulls_last(),
    )
)
