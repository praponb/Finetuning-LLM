from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"), "island_id")
    .agg(
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.percentile_approx("duration_sec", 0.9).alias("duration_sec_p90"),
        F.max("event_time").alias("ended_at"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_rows"),
    )
)
