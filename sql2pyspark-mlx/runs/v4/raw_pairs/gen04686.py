from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("temp_c") >= 33.4, "HIGH").otherwise("LOW").alias("temp_c_tier"),
        F.when(F.col("status_bits") >= 10, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
        F.when(F.col("pressure") >= 1018, "PREMIUM").otherwise("STANDARD").alias("pressure_tier"),
    )
    .agg(
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("temp_c"))).alias("dv_04_total_temp_c"),
        F.max("status_bits").alias("max_status_bits"),
        F.countDistinct("firmware").alias("distinct_firmwares_2"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
)
