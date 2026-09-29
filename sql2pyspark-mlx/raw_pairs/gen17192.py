from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        "device_code",
        F.when(F.col("temp_c") >= 39.0, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
        F.date_trunc("day", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("pressure")).otherwise(0)).alias("dv_02_pressure"),
    )
    .orderBy("dv_03_events")
)
