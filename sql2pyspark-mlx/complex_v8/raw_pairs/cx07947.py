from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_starts_session_full = Window.orderBy(F.col("starts_session").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_starts_session_ties = Window.orderBy(F.col("starts_session").desc())
w_event_type_2 = Window.partitionBy("event_type")
w_all_by_starts_session = Window.orderBy(F.col("starts_session").desc(), "id")

base_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = base_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_event_type_running))

screened = (
    sessionized.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "replied") & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-18").cast("date")), "left_semi")
)

sequenced = (
    screened
    .select(
        "id",
        "ticket_id",
        "event_type",
        "prev_event_at",
        "starts_session",
        "burst_id",
        F.nth_value("event_at", 2).over(w_all_by_starts_session_full).alias("second_event_at"),
        F.round(F.cume_dist().over(w_all_by_starts_session_ties), 3).alias("starts_session_cume_dist"),
        F.count(F.lit(1)).over(w_event_type_2).alias("rows_in_event_type"),
        F.lag("event_at").over(w_all_by_starts_session).alias("prev_event_at_2"),
    )
)

result = (
    sequenced
    .filter(F.col("burst_id") >= 2)
    .select(
        "id",
        "event_type",
        "prev_event_at",
        "starts_session",
        "burst_id",
        "second_event_at",
        "starts_session_cume_dist",
        "rows_in_event_type",
        "prev_event_at_2",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy(F.col("id").desc())
)
