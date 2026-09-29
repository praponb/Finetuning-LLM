from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "session_no",
        F.when(F.col("event_type").rlike("^(click|search)$"), "PRIORITY").when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("event_type_zone"),
    )
)
