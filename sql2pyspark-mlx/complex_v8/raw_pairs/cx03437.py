from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_agent_team_by_agent_hi = Window.partitionBy("actor", "agent_team").orderBy(F.col("agent_hired_on").desc(), "id")
w_agent_team_by_event_at_ties = Window.partitionBy("agent_team").orderBy(F.col("event_at").asc_nulls_last())
w_event_type_by_agent_hired_on = Window.partitionBy("event_type").orderBy(F.col("agent_hired_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_agent_hired_on_2 = Window.partitionBy("event_type").orderBy(F.col("agent_hired_on").asc_nulls_last(), "id")
w_actor_agent_team_by_event_at = Window.partitionBy("actor", "agent_team").orderBy("event_at")
w_actor_agent_team_by_event_at_2 = Window.partitionBy("actor", "agent_team").orderBy("event_at", "id")
w_agent_team_by_event_at = Window.partitionBy("agent_team").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_agent_hired_on_year_by_event = Window.partitionBy(F.year("agent_hired_on")).orderBy("event_at", "id")
w_agent_hired_on_year_by_event_2 = Window.partitionBy(F.year("agent_hired_on")).orderBy("event_at")
w_agent_team_by_event_at_runni = Window.partitionBy("agent_team").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    enriched_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_priority",
        "ticket_tags",
        "agent_hired_on",
        F.round(F.percent_rank().over(w_actor_agent_team_by_agent_hi), 3).alias("agent_hired_on_percent_rank"),
        F.round(F.percent_rank().over(w_agent_team_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.rank().over(w_actor_agent_team_by_agent_hi).alias("agent_hired_on_rank"),
        F.first("event_at").over(w_event_type_by_agent_hired_on).alias("first_event_at"),
        F.ntile(10).over(w_event_type_by_agent_hired_on_2).alias("agent_hired_on_decile"),
        F.rank().over(w_actor_agent_team_by_event_at).alias("event_at_rank"),
        F.lead("event_at", 3).over(w_actor_agent_team_by_event_at_2).alias("next3_event_at"),
        F.lead("agent_hired_on").over(w_agent_team_by_event_at).alias("next_agent_hired_on"),
        F.ntile(3).over(w_agent_team_by_event_at).alias("event_at_tercile"),
        F.dense_rank().over(w_agent_hired_on_year_by_event).alias("event_at_dense_rank"),
        F.lag("agent_hired_on").over(w_agent_hired_on_year_by_event).alias("prev_agent_hired_on"),
        F.lead("event_at").over(w_actor_agent_team_by_event_at_2).alias("next_event_at"),
        F.round(F.cume_dist().over(w_agent_hired_on_year_by_event_2), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_agent_team_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.first("agent_hired_on", ignorenulls=True).over(w_agent_team_by_event_at_runni).alias("first_known_agent_hired_on"),
    )
)

result = (
    metrics
    .filter(F.col("first_known_agent_hired_on") >= F.lit("2024-07-09").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_priority",
        "ticket_tags",
        "agent_hired_on_percent_rank",
        "event_at_percent_rank",
        "first_event_at",
        "agent_hired_on_decile",
        "event_at_rank",
        "next_agent_hired_on",
        "event_at_tercile",
        "event_at_dense_rank",
        "prev_agent_hired_on",
        "next_event_at",
        "event_at_cume_dist",
        "minutes_since_prev_event_at",
        "first_known_agent_hired_on",
        F.size(F.col("ticket_tags")).alias("ticket_tags_count"),
    )
    .orderBy(F.col("ticket_tags_count").asc_nulls_last(), "id")
    .limit(10)
)
