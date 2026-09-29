from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("device_code")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("pressure")).otherwise(0)).alias("dv_01_pressure"),
        F.max(F.when(F.col("device_code") == "DV-02", F.col("pressure"))).alias("dv_02_max_pressure"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
    )
    .orderBy(F.col("temp_c_stddev").desc_nulls_last())
)
