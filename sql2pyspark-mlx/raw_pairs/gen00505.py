from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .withColumn("device_code_upper", F.upper("device_code"))
    .withColumn("reading_ts_hour", F.hour("reading_ts"))
)

result = (
    cleaned
    .select("device_code", "firmware")
    .intersect(
        df_device_readings
        .filter(F.col("humidity") < 47)
        .select("device_code", "firmware")
    )
)
