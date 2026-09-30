from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_by_event_at_running = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor").isin("customer", "agent")) & (F.col("event_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

metrics = (
    cleaned_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.last("event_at").over(w_actor_by_event_at_full).alias("last_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.first("event_at", ignorenulls=True).over(w_actor_by_event_at_running).alias("first_known_event_at"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "resolved") & (F.col("t.event_at") < F.lit("2025-03-02 15:00:00").cast("timestamp")), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "last_event_at",
        "minutes_since_prev_event_at",
        "first_known_event_at",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)
