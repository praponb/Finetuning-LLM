from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 09:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round("humidity", 0).alias("humidity_rounded"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("temp_c")).otherwise(0)).alias("dv_02_temp_c"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.percentile_approx("temp_c", 0.95).alias("temp_c_p95"),
    )
    .orderBy(F.col("temp_c_p95").desc())
)
