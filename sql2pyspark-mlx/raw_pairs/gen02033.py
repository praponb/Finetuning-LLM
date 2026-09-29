from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.hour("reading_ts").between(11, 18))
    .withColumn("temp_c_whole", F.floor("temp_c"))
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .cube("activity_day", "hour_window")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.corr("humidity", "temp_c").alias("corr_humidity_temp_c"),
        F.grouping_id().alias("grouping_level"),
    )
)
