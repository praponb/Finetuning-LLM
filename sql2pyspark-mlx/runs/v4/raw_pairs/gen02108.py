from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 35.8, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity"))).alias("dv_02_total_humidity"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
    .filter(F.col("status_bits_bit_and") < 1)
    .orderBy(F.col("temp_c_tier").desc_nulls_first())
)
