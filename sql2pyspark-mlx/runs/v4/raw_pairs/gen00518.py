from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp")) & (F.col("humidity") < 38))
    .withColumn("status_bits_adjusted", F.round((F.col("status_bits") * 100), 2))
)

result = (
    valid
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.min("reading_ts").alias("first_seen"),
    )
)
