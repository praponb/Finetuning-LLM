from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_ticket_events
    .filter((F.col("actor") == "agent") & (F.month(F.col("event_at")).isin(2, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_type",
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
    )
)

derived = (
    metrics
    .withColumn("event_type_or_unknown", F.coalesce(F.col("event_type"), F.lit("UNKNOWN")))
)

with_children_events = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_at_cume_dist",
        "s.prev_event_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("actor") == "system", 1)).alias("event_actor_count"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_counts = (
    with_children_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.first_event_at_2",
    )
)

result = (
    with_counts
    .filter(F.col("first_event_at_2") < F.lit("2025-02-17 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_at_cume_dist",
        "prev_event_at",
        "event_id",
        "event_actor",
        "first_event_at",
        "event_actor_count",
        F.to_date(F.col("first_event_at_2")).alias("first_event_at_2_date"),
    )
)
