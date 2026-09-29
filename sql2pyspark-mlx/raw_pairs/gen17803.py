from pyspark.sql import functions as F

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY"),
        F.col("pressure").alias("PRESSURE"),
        "temp_c",
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["HUMIDITY", "PRESSURE", "temp_c"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)
