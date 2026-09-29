from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_dayofweek", F.dayofweek("reading_ts"))
)

result = (
    valid
    .select(
        "id",
        "device_code",
        "humidity",
        F.col("pressure").alias("PRESSURE"),
        F.col("temp_c").alias("TEMP_C"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity", "PRESSURE", "TEMP_C"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
