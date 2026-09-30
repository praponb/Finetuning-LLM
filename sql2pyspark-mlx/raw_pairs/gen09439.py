from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("web_event_event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("web_event_event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("event_time", "id")
w5 = Window.partitionBy("customer_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.event_time"),
    )
)

prev_events = latest_match.withColumn("prev_event_time", F.lag("event_time").over(w2))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w3))

gaps_2 = (
    islands
    .withColumn("is_new_session_2", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("burst_id", F.sum("is_new_session_2").over(w5))

with_dims = (
    sessionized
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.age"), F.col("b.email"), F.col("b.segment"))
)

result = (
    with_dims
    .withColumn("quarter_start", F.date_trunc("QUARTER", "prev_event_time"))
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .groupingSets(
        [["quarter_start", "burst_id_tier", "year_start"], ["quarter_start", "burst_id_tier"]],
        "quarter_start",
        "burst_id_tier",
        "year_start",
    )
    .agg(
        F.sum(F.when(F.col("device") == "desktop", F.col("is_new_session_2")).otherwise(0)).alias("desktop_is_new_session_2"),
        F.max("event_time").alias("ended_at"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.avg("is_new_session_2").alias("avg_is_new_session_2"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .orderBy("avg_is_new_session_2")
)
