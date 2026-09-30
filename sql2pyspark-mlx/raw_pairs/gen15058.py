from pyspark.sql import functions as F

remaining_keys = (
    df_device_readings
    .select("device_code", "firmware")
    .intersectAll(
        df_device_readings
        .filter(F.col("humidity").between(38, 41))
        .select("device_code", "firmware")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("status_bits") >= 11, "HIGH").otherwise("LOW").alias("status_bits_tier"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("status_bits")).otherwise(0)).alias("dv_03_status_bits"),
        F.min("reading_ts").alias("started_at"),
    )
    .orderBy(F.col("status_bits_tier").desc())
)
