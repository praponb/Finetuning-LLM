from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2023-04-01")
    .withColumn("firmware_upper", F.upper("firmware"))
)

result = (
    cleaned
    .select(
        "id",
        "device_code",
        F.col("temp_c").alias("TEMP_C"),
        F.col("humidity").alias("HUMIDITY_VAL"),
        F.col("pressure").alias("PRESSURE"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["TEMP_C", "HUMIDITY_VAL", "PRESSURE"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
