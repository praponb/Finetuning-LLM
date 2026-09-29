from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 09:00:00").cast("timestamp")) & F.col("humidity").isNull())
    .withColumn("status_bits_plus_pressure", (F.col("status_bits") + F.col("pressure")))
)

result = (
    cleaned
    .select(
        "id",
        "device_code",
        F.col("status_bits_plus_pressure").alias("STATUS_BITS_PLUS_PRESSURE"),
        F.col("temp_c").alias("TEMP_C"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["STATUS_BITS_PLUS_PRESSURE", "TEMP_C"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
