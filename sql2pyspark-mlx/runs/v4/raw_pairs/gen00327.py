from pyspark.sql import functions as F

result = (
    df_device_readings
    .rollup(
        F.when(F.col("temp_c") >= 41.7, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
        F.when(F.col("humidity") >= 83, "HIGH").otherwise("LOW").alias("humidity_tier"),
    )
    .agg(
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list_2"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
    .orderBy(F.col("temp_c_tier").desc_nulls_first())
)
