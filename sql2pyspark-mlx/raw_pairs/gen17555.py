from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 03:00:00").cast("timestamp")) & F.col("humidity").between(59, 76))
    .select(
        "*",
        F.greatest("humidity", "temp_c").alias("greatest_humidity_temp_c"),
        F.ceil("status_bits").alias("status_bits_whole"),
    )
)

result = (
    cleaned
    .select("id", "device_code", "humidity", "greatest_humidity_temp_c", "temp_c")
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity", "greatest_humidity_temp_c", "temp_c"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
