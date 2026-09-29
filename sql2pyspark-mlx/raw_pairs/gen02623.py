from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.col("pressure").cast("int").alias("pressure_int"),
    )
)

result = (
    filtered
    .select("id", "device_code", "pressure", "temp_c")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "temp_c"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
