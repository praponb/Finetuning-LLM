from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("pressure"))).alias("dv_04_avg_pressure"),
        F.count("*").alias("row_count"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
    )
    .orderBy("hour_window")
)
