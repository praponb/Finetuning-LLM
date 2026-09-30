from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .groupBy(
        "page",
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("is_new_burst")).otherwise(0)).alias("add_to_cart_is_new_burst"),
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy("row_count")
)
