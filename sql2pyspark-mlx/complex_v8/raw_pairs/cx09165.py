from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_event_at_date_month_by = Window.partitionBy("actor", F.month("event_at_date")).orderBy("event_at")
w_actor_event_at_date_month_by_2 = Window.partitionBy("actor", F.month("event_at_date")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_date_month_by_3 = Window.partitionBy("actor", F.month("event_at_date")).orderBy("event_at", "id")
w_actor_agent_team_by_event_at = Window.partitionBy("actor", "agent_team").orderBy(F.col("event_at_date").desc(), "id")
w_actor_agent_team_by_event_at_2 = Window.partitionBy("actor", "agent_team").orderBy(F.col("event_at_date").desc())
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_agent_team_by_event_at_3 = Window.partitionBy("actor", "agent_team").orderBy(F.col("event_at_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_at_date_month_by_4 = Window.partitionBy("actor", F.month("event_at_date")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "customer")) & (~F.col("event_type").isin("replied", "created")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_parent_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

sequenced = (
    with_parent_ticket
    .select(
        "*",
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.round(F.cume_dist().over(w_actor_event_at_date_month_by), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_actor_event_at_date_month_by_2).alias("second_event_at"),
        F.round(F.percent_rank().over(w_actor_event_at_date_month_by_3), 3).alias("event_at_percent_rank"),
        F.ntile(3).over(w_actor_event_at_date_month_by_3).alias("event_at_tercile"),
        F.ntile(5).over(w_actor_agent_team_by_event_at).alias("event_at_date_quintile"),
        F.round(F.percent_rank().over(w_actor_agent_team_by_event_at_2), 3).alias("event_at_date_percent_rank"),
        F.datediff(F.col("event_at_date"), F.lag("event_at_date").over(w_all_by_event_at)).alias("days_since_prev_event_at_date"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at_2"),
        F.last("event_at", ignorenulls=True).over(w_actor_agent_team_by_event_at_3).alias("last_known_event_at"),
        F.lag("event_at", 3).over(w_all_by_event_at).alias("prev3_event_at"),
        F.first("event_at_date", ignorenulls=True).over(w_actor_event_at_date_month_by_4).alias("first_known_event_at_date"),
        F.nth_value("event_at_date", 2).over(w_all_by_event_at_full).alias("second_event_at_date"),
    )
)

annotated = (
    sequenced
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_clean")), " ", "_")).alias("event_type_clean_slug"),
    )
)

result = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_date",
        "event_type_clean",
        "ticket_tags",
        "agent_tier",
        "event_at_seq",
        "event_at_cume_dist",
        "event_at_percent_rank",
        "event_at_tercile",
        "event_at_date_quintile",
        "days_since_prev_event_at_date",
        "second_event_at_2",
        "prev3_event_at",
        "first_known_event_at_date",
        "second_event_at_date",
        F.coalesce(F.col("agent_tier"), F.lit("UNKNOWN")).alias("agent_tier_or_unknown"),
    )
)
