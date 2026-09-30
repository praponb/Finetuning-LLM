from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        F.when(F.col("event_type").rlike("^(click|page_view)$"), "GROUP_B").otherwise("PRIORITY").alias("event_type_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last(), F.col("event_type_zone").desc())
)
