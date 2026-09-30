from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("clean_device_code", F.trim("device_code"))
)

result = (
    base
    .groupBy(
        F.when(F.col("pressure") >= 1024, "PREMIUM").otherwise("STANDARD").alias("pressure_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.max(F.when(F.col("device_code") == "DV-01", F.col("status_bits"))).alias("dv_01_max_status_bits"),
        F.avg(F.when(F.col("device_code") == "DV-01", F.col("pressure"))).alias("dv_01_avg_pressure"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("status_bits"))).alias("dv_02_total_status_bits"),
        F.min("humidity").alias("min_humidity"),
    )
    .filter(F.col("dv_02_total_status_bits") < 1000000)
    .orderBy(F.col("dv_01_avg_pressure").asc_nulls_last())
)
