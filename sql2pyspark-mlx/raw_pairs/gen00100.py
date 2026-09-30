from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "pressure", "temp_c", "humidity")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "temp_c", "humidity"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
