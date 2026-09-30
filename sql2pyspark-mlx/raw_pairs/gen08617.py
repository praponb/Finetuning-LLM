from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") < 11)
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.upper("firmware").alias("firmware_upper"),
    )
)

result = (
    cleaned
    .select("id", "temp_c", "humidity", "pressure")
    .unpivot(
        ids=["id"],
        values=["temp_c", "humidity", "pressure"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
