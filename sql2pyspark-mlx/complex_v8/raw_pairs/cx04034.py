from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

sequenced = (
    cleaned_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_or_unknown",
        F.round(F.percent_rank().over(w_all_by_event_at), 3).alias("event_at_percent_rank"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor_or_unknown",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    with_details
    .groupingSets(
        [["ticket_id", "actor_or_unknown"], ["ticket_id"], ["actor_or_unknown"]],
        "ticket_id", "actor_or_unknown",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.max("ticket_closed_at").alias("last_ticket_closed_at"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor_or_unknown").asc_nulls_last(),
    )
)
