from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_year_by_ticket_open = Window.partitionBy(F.year("event_at")).orderBy("ticket_opened_at", "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_type_event_at_to_date = Window.partitionBy("event_type", F.to_date("event_at")).orderBy(F.col("event_at").asc_nulls_last())
w_all_by_ticket_opened_at = Window.orderBy(F.col("ticket_opened_at").desc(), "id")

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 45)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_parent_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_label",
        "s.event_type_actor_label",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

with_trends = (
    with_parent_ticket
    .select(
        "*",
        F.rank().over(w_event_at_year_by_ticket_open).alias("ticket_opened_at_rank"),
        F.lag("event_at").over(w_event_at_year_by_ticket_open).alias("prev_event_at"),
        F.lead("ticket_opened_at", 3).over(w_all_by_event_at).alias("next3_ticket_opened_at"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at_2"),
        F.lead("event_at").over(w_event_at_year_by_ticket_open).alias("next_event_at"),
        F.rank().over(w_event_type_event_at_to_date).alias("event_at_rank"),
        F.lag("ticket_opened_at", 2).over(w_all_by_ticket_opened_at).alias("prev2_ticket_opened_at"),
        F.ntile(10).over(w_all_by_ticket_opened_at).alias("ticket_opened_at_decile"),
    )
)

stats = (
    with_trends
    .groupBy("agent_team")
    .agg(
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
        F.sum("ticket_opened_at_decile").alias("total_ticket_opened_at_decile"),
        F.min("prev2_ticket_opened_at").alias("first_prev2_ticket_opened_at"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .filter(F.col("total_ticket_opened_at_decile") > 19)
    .select(
        "agent_team",
        "distinct_agent_teams",
        "total_ticket_opened_at_decile",
        "cnt",
        F.length(F.trim(F.col("agent_team"))).alias("agent_team_len"),
    )
    .orderBy("cnt")
)
