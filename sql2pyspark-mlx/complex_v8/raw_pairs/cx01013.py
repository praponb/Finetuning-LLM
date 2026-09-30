from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 53)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_prev = valid_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_event_type_running))

summary = (
    sessionized
    .groupBy(
        F.date_trunc("month", "prev_event_at").alias("prev_event_at_month"),
        "ticket_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.sum("days_since_event_at").alias("total_days_since_event_at"),
    )
)

result = (
    summary
    .select(
        "prev_event_at_month",
        "ticket_id",
        "cnt",
        "total_days_since_event_at",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        F.to_date(F.col("prev_event_at_month")).alias("prev_event_at_month_date"),
    )
    .orderBy(
        F.col("cnt_bucket").desc_nulls_first(),
        F.col("ticket_id").desc_nulls_first(),
        "prev_event_at_month",
    )
    .limit(5)
)
