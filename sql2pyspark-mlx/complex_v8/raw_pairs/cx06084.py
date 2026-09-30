from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at_prior_2 = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(-2, -1)
w_actor_len = Window.partitionBy("actor_len")

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.last("actor_len", ignorenulls=True).over(w_actor_by_event_at_running).alias("last_known_actor_len"),
        F.max("actor_len").over(w_actor_by_event_at_prior_2).alias("actor_len_max_2_prior"),
        F.round(F.avg("actor_len").over(w_actor_len), 2).alias("actor_len_avg_actor_len"),
    )
)

combined_details = (
    metrics.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor_len",
        "s.actor_slug",
        "s.last_known_actor_len",
        "s.actor_len_avg_actor_len",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

screened = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

combined_details_ticket = (
    screened.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id") >= 4)
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor_len",
        "s.actor_slug",
        "s.actor_len_avg_actor_len",
        "s.ticket_requester_id",
        F.col("t.opened_at").alias("ticket_opened_at_2"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "actor_len",
        "actor_slug",
        "actor_len_avg_actor_len",
        "ticket_requester_id",
        "ticket_opened_at_2",
        F.abs(F.col("ticket_requester_id") - 9).alias("ticket_requester_id_distance"),
    )
    .orderBy("ticket_opened_at_2", F.col("ticket_requester_id").desc())
)
