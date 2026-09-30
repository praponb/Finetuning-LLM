from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 06:00:00")))
    .select(
        "*",
        F.concat("firmware", F.lit(" "), "device_code").alias("firmware_device_code"),
    )
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY"),
        F.col("pressure").alias("PRESSURE_VAL"),
        F.col("temp_c").alias("TEMP_C_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["HUMIDITY", "PRESSURE_VAL", "TEMP_C_VAL"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
