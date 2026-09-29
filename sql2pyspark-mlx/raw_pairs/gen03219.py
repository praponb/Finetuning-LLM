from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.sum("is_new_burst").over(w2).alias("session_id_2"),
    )
)

result = (
    sessionized
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "session_id_2",
        F.when(F.col("event_type").rlike("^(purchase|page_view)$"), "GROUP_A").when(F.col("event_type").rlike("^(click|add_to_cart)$"), "PRIORITY").otherwise("EXTENDED").alias("event_type_zone"),
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy("sum_web_event_duration_sec")
)
