from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("session_id_2") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_id_2_tier"),
        "event_type",
    )
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
    )
    .filter(F.col("search_rows") >= 1)
)
