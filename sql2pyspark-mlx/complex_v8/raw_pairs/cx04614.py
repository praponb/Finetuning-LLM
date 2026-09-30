from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")

filtered_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

ranked = (
    filtered_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

detail_rows_events = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("resolved", "assigned", "escalated")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.minutes_since_prev_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    detail_rows_events
    .filter(F.col("event_at") < F.lit("2025-03-08 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_event_type",
        "event_actor",
    )
    .orderBy("event_event_type", F.col("ticket_id").desc_nulls_first())
)
