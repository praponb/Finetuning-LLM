from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("temp_c_tier", F.when(F.col("temp_c") >= 37.3, "PREMIUM").otherwise("STANDARD"))
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 74, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["temp_c_tier", "humidity_tier"], ["temp_c_tier"], []],
        "temp_c_tier",
        "humidity_tier",
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.round(F.sum("pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("pressure_per_dv_04"),
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("status_bits_per_dv_03"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity")).otherwise(0)).alias("dv_02_humidity"),
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
        F.size(F.collect_set("device_code")).alias("num_device_codes_2"),
    )
    .filter(F.col("avg_status_bits") > 0)
    .orderBy(F.col("avg_status_bits").asc_nulls_last())
)
