from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "temp_c", F.col("pressure").alias("PRESSURE_VAL"))
    .unpivot(
        ids=["id", "device_code"],
        values=["temp_c", "PRESSURE_VAL"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
