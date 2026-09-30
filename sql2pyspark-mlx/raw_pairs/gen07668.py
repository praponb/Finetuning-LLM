from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 08:00:00"))) & (F.col("humidity") <= 64))
    .select(
        "*",
        (F.col("temp_c") + F.col("humidity")).alias("temp_c_plus_humidity"),
        F.coalesce("temp_c", F.lit(0)).alias("temp_c_filled"),
    )
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "temp_c_plus_humidity",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("temp_c_plus_humidity") >= 82, "PREMIUM").otherwise("STANDARD").alias("temp_c_plus_humidity_tier"),
    )
    .agg(
        F.max("reading_ts").alias("last_seen"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
