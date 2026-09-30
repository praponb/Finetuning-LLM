from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "island_id",
        F.when(F.col("event_type").rlike("^(click|add_to_cart)$"), "EXTENDED").when(F.col("event_type").rlike("^(page_view|purchase)$"), "CORE").otherwise("GROUP_A").alias("event_type_zone"),
    )
)
