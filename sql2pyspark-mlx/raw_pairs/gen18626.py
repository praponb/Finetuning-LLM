from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("temp_c").between(29, 37))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("humidity") / F.nullif("status_bits", F.lit(0))).alias("humidity_per_status_bits"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY"),
        F.col("temp_c").alias("TEMP_C"),
        F.col("pressure").alias("PRESSURE"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["HUMIDITY", "TEMP_C", "PRESSURE"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
