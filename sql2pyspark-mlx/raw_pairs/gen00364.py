from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 38.0, "HIGH").otherwise("LOW").alias("temp_c_tier"),
        "device_code",
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events_2"),
        F.max("pressure").alias("max_pressure"),
    )
    .filter(F.col("dv_02_events") >= 1)
)
