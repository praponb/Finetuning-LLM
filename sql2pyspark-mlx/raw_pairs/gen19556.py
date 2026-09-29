from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "humidity", "temp_c", "pressure")
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity", "temp_c", "pressure"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
)
