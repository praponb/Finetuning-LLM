from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 12, "PREMIUM").otherwise("STANDARD"))
    .cube("activity_day", "status_bits_tier")
    .agg(
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.percentile_approx("status_bits", 0.5).alias("status_bits_p50"),
        F.grouping_id().alias("grouping_id"),
    )
)
