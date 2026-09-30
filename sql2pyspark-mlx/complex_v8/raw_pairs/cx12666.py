from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")

valid_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        F.col("t.tags").alias("ticket_tags"),
    )
)

with_windows = (
    enriched_ticket
    .select(
        "id",
        "ticket_id",
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["ticket_id", "event_at_seq"], ["ticket_id"]],
        "ticket_id", "event_at_seq",
    )
    .agg(
        F.sum("event_at_seq").alias("total_event_at_seq"),
        F.round(F.stddev("event_at_seq"), 2).alias("stddev_event_at_seq"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_at_seq").asc_nulls_last(),
    )
)
