from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "temp_c", "humidity", "pressure")
    .unpivot(
        ["id", "device_code"],
        ["temp_c", "humidity", "pressure"],
        "metric_name",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
