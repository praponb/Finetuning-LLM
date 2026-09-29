from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.count("*").alias("cnt"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("event_type") == "search", 1)), F.lit(0)), 2).alias("session_start_flag_per_search"),
    )
)
