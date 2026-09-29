from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("quarter_start").orderBy(F.col("num_pages").desc())

flagged = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.sum("is_new_burst").over(w2).alias("session_id_2"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
        F.date_trunc("week", "event_time").alias("week_start"),
    )
    .agg(
        F.round(F.sum("session_id_2") / F.nullif(F.count(F.when(F.col("event_type") == "click", 1)), F.lit(0)), 2).alias("session_id_2_per_click"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("page")).alias("num_pages"),
    )
    .withColumn("num_pages_dense_rank", F.dense_rank().over(w3))
    .orderBy(F.col("quarter_start").desc())
)
