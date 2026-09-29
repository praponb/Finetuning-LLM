from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("temp_c") <= 21)
    .withColumn("humidity_scaled", F.round((F.col("humidity") * 100), 2))
)

gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy("session_id", F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("temp_c")).otherwise(0)).alias("dv_03_temp_c"),
        F.max("reading_ts").alias("last_seen"),
        F.count("*").alias("event_count"),
    )
)
