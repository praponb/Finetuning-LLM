from pyspark.sql import functions as F

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("pressure").alias("PRESSURE_VAL"),
        F.col("humidity").alias("HUMIDITY_VAL"),
        "temp_c",
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["PRESSURE_VAL", "HUMIDITY_VAL", "temp_c"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
