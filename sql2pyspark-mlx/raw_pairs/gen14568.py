from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        "device_code",
        F.when(F.col("humidity") >= 71, "HIGH").otherwise("LOW").alias("humidity_tier"),
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.percentile_approx("pressure", 0.9).alias("pressure_p90"),
    )
    .orderBy(F.col("device_code").desc_nulls_last())
)
