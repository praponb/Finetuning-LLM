from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
)

with_prev = base.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "days_since_event_time",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 514, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .groupingSets(
        [["session_no", "duration_sec_tier", "activity_month"], ["session_no", "duration_sec_tier"], []],
        "session_no",
        "duration_sec_tier",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("duration_sec_per_product"),
        F.max("event_time").alias("last_seen"),
    )
)
