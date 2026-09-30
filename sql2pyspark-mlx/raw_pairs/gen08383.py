from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "pressure", F.col("humidity").alias("HUMIDITY"))
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "HUMIDITY"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
