from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select("*", F.col("humidity").cast("int").alias("humidity_int"))
)

result = (
    cleaned
    .select(
        "id",
        "device_code",
        F.col("temp_c").alias("TEMP_C"),
        F.col("humidity").alias("HUMIDITY"),
        F.col("pressure").alias("PRESSURE"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["TEMP_C", "HUMIDITY", "PRESSURE"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
