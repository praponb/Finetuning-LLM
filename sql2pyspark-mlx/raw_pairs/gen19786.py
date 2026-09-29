from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("quarter_start").orderBy(F.col("num_event_types").desc())

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.sum("is_new_burst").over(w2).alias("session_id_2"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"), "event_type")
    .agg(
        F.avg("session_id_2").alias("avg_session_id_2"),
        F.countDistinct("page").alias("distinct_pages"),
        F.max("session_id_2").alias("max_session_id_2"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
    )
    .withColumn("num_event_types_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("search_rows").asc_nulls_first())
)
