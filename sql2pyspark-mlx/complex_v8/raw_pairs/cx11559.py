from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team_by_event_at = Window.partitionBy("agent_team").orderBy("event_at", "id")
w_agent_team_by_event_at_ties = Window.partitionBy("agent_team").orderBy("event_at")
w_agent_team_event_type_by_age = Window.partitionBy("agent_team", "event_type").orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_hired_on_month_by_agen = Window.partitionBy(F.date_trunc("month", "agent_hired_on")).orderBy("agent_hired_on", "id")
w_agent_team_event_type_by_age_2 = Window.partitionBy("agent_team", "event_type").orderBy(F.col("agent_hired_on").desc())
w_agent_hired_on_month_by_agen_2 = Window.partitionBy(F.date_trunc("month", "agent_hired_on")).orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")

valid = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent_ticket = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    with_parent_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_agent_id",
        "ticket_closed_at",
        "agent_hired_on",
        F.lead("agent_hired_on").over(w_agent_team_by_event_at).alias("next_agent_hired_on"),
        F.rank().over(w_agent_team_by_event_at_ties).alias("event_at_rank"),
        F.lead("agent_hired_on").over(w_agent_team_event_type_by_age).alias("next_agent_hired_on_2"),
        F.ntile(3).over(w_agent_team_by_event_at).alias("event_at_tercile"),
        F.nth_value("agent_hired_on", 2).over(w_all_by_event_at_full).alias("second_agent_hired_on"),
        F.lead("ticket_closed_at", 3).over(w_agent_hired_on_month_by_agen).alias("next3_ticket_closed_at"),
        F.rank().over(w_agent_team_event_type_by_age_2).alias("agent_hired_on_rank"),
        F.first("agent_hired_on").over(w_agent_hired_on_month_by_agen_2).alias("first_agent_hired_on"),
        F.last("ticket_closed_at").over(w_all_by_event_at_full).alias("last_ticket_closed_at"),
        F.lead("event_at").over(w_agent_team_event_type_by_age).alias("next_event_at"),
        F.dense_rank().over(w_agent_hired_on_month_by_agen).alias("agent_hired_on_dense_rank"),
        F.lead("event_at", 3).over(w_all_by_event_at).alias("next3_event_at"),
        F.ntile(4).over(w_agent_team_by_event_at).alias("event_at_quartile"),
        F.round(F.percent_rank().over(w_agent_team_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

top_rows = (
    ranked
    .filter(F.col("event_at_rank") <= 5)
    .drop("event_at_rank")
)

result = (
    top_rows
    .filter(F.year(F.col("agent_hired_on")) == 2024)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_closed_at",
        "agent_hired_on",
        "next_agent_hired_on",
        "next_agent_hired_on_2",
        "event_at_tercile",
        "second_agent_hired_on",
        "agent_hired_on_rank",
        "last_ticket_closed_at",
        "next_event_at",
        "next3_event_at",
        "event_at_quartile",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
    .orderBy(F.col("last_ticket_closed_at").desc_nulls_first())
)
