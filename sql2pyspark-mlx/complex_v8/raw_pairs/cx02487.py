from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

filtered = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

with_activity = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    with_activity
    .rollup("quality_len", "site_id")
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
    )
    .orderBy(
        F.col("quality_len").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
