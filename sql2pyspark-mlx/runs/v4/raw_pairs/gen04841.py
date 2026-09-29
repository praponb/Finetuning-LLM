from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("humidity") >= 81, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.min("reading_ts").alias("first_seen"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.size(F.collect_set("device_code")).alias("num_device_codes_2"),
    )
)
