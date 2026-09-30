from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_device_readings
    .groupBy("island_id")
    .agg(
        F.avg("island_id").alias("avg_island_id"),
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("is_new_session")).otherwise(0)).alias("dv_01_is_new_session"),
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
    )
    .filter(F.col("avg_island_id") < 0)
    .orderBy("event_count")
)
