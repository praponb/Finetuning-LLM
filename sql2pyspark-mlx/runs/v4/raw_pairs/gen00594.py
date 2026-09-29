from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("humidity") >= 78, "PREMIUM").otherwise("STANDARD").alias("humidity_tier"),
    )
    .agg(
        F.countDistinct("firmware").alias("distinct_firmwares"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
    )
    .filter(F.col("distinct_firmwares") >= 1)
    .orderBy(F.col("distinct_firmwares").desc_nulls_first())
)
