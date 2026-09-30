from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 80, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["activity_day", "humidity_tier"], ["activity_day"]],
        "activity_day",
        "humidity_tier",
    )
    .agg(
        F.round(F.sum("temp_c") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("temp_c_per_dv_04"),
        F.percentile_approx("status_bits", 0.5).alias("status_bits_p50"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("activity_day").desc())
)
