from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_date = Window.orderBy(F.col("event_at_date").desc_nulls_first(), "id", "event_id")

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 16) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

exploded_detail = (
    base_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("escalated", "created", "replied")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_date",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

with_windows = (
    exploded_detail
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_id",
        "event_event_type",
        "event_event_at",
        F.datediff(F.col("event_at_date"), F.lag("event_at_date").over(w_all_by_event_at_date)).alias("days_since_prev_event_at_date"),
    )
)

result = (
    with_windows
    .filter(F.col("event_event_type").isin("reopened", "escalated", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_id",
        "days_since_prev_event_at_date",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("event_event_at"))) / 60, 1).alias("minutes_event_event_at_to_event_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_event_at")).alias("days_since_event_event_at"),
    )
    .orderBy(F.col("event_id").desc(), "id")
    .limit(5)
)
