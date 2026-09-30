from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
        F.when(F.col("status_bits") >= 13, "HIGH").otherwise("LOW").alias("status_bits_tier"),
    )
    .agg(
        F.max("reading_ts").alias("last_reading_ts"),
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
        F.percentile_approx("humidity", 0.9).alias("humidity_p90"),
    )
)
