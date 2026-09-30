from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
    .orderBy(F.col("island_id_p95").desc_nulls_first())
)
