from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", F.col("humidity").alias("HUMIDITY"), "pressure")
    .unpivot(
        ids=["id", "device_code"],
        values=["HUMIDITY", "pressure"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
