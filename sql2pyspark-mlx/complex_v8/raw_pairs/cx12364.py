from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_agent_id = Window.partitionBy("ticket_agent_id")
w_actor_ticket_csat_by_event_a = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("event_at").desc())
w_actor_ticket_csat_by_event_a_2 = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_year_ticket_id_by_t = Window.partitionBy(F.year("event_at"), "ticket_id").orderBy(F.col("ticket_csat").desc())
w_actor_ticket_csat_by_event_a_3 = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("replied", "created", "resolved")) & (F.col("event_at") < F.lit("2025-02-06 12:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
    )
)

with_details_ticket = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.ticket_csat",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

derived = (
    with_details_ticket
    .select("*", F.array_contains(F.col("ticket_tags"), "login").alias("has_login"))
)

with_trends = (
    derived
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_csat",
        "ticket_agent_id",
        "ticket_channel",
        "ticket_tags",
        "has_login",
        F.count(F.lit(1)).over(w_ticket_agent_id).alias("rows_in_ticket_agent_id"),
        F.round(F.cume_dist().over(w_actor_ticket_csat_by_event_a), 3).alias("event_at_cume_dist"),
        F.lag("event_at").over(w_actor_ticket_csat_by_event_a_2).alias("prev_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.round(F.percent_rank().over(w_event_at_year_ticket_id_by_t), 3).alias("ticket_csat_percent_rank"),
        F.first("ticket_csat").over(w_actor_ticket_csat_by_event_a_3).alias("first_ticket_csat"),
    )
)

result = (
    with_trends
    .filter(F.col("rows_in_ticket_agent_id") < 13)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_agent_id",
        "ticket_channel",
        "ticket_tags",
        "has_login",
        "rows_in_ticket_agent_id",
        "event_at_cume_dist",
        "prev_event_at",
        "second_event_at",
        "ticket_csat_percent_rank",
        "first_ticket_csat",
        F.when(F.col("has_login"), 1).otherwise(0).alias("has_login_flag"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "prev_event_at")
)
