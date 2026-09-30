from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "humidity", "pressure", "temp_c")
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity", "pressure", "temp_c"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
