from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .rollup("activity_day", "device_code")
    .agg(
        F.max("reading_ts").alias("ended_at"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.min("reading_ts").alias("first_seen"),
        F.grouping_id().alias("grouping_level"),
    )
)
