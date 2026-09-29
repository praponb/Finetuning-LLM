from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("is_new_burst").over(w2).alias("session_id_2"),
    )
)

result = (
    sessions_web_events
    .withColumn("week_start", F.date_trunc("week", "event_time"))
    .withColumn("year_start", F.date_trunc("year", "event_time"))
    .rollup("week_start", "year_start")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("cart_events") > 1)
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
