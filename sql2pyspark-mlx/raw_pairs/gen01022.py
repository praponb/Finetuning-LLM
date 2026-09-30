from pyspark.sql import functions as F

long_format = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("temp_c").alias("TEMP_C_VAL"),
        F.col("pressure").alias("PRESSURE"),
        F.col("humidity").alias("HUMIDITY"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["TEMP_C_VAL", "PRESSURE", "HUMIDITY"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    long_format
    .groupBy(
        F.when(F.col("val") >= 65, "LARGE").otherwise("SMALL").alias("val_tier"),
        "metric_name",
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.avg(F.when(F.col("device_code") == "DV-03", F.col("val"))).alias("dv_03_avg_val"),
    )
)
