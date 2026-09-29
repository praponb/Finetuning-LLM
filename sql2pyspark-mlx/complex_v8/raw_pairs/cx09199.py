from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_all_by_days_since_opened_at = Window.orderBy("days_since_opened_at")
w_all_by_days_since_opened_at_2 = Window.orderBy("days_since_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter((F.col("channel") == "email") | (F.col("closed_at") >= F.lit("2025-02-02 03:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

filtered = (
    cleaned_tickets
    .crossJoin(cleaned_tickets.agg(F.avg("days_since_opened_at").alias("overall_avg_days_since_opened_at")))
    .filter(F.col("days_since_opened_at") > F.col("overall_avg_days_since_opened_at"))
    .drop("overall_avg_days_since_opened_at")
)

screened = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.month(F.col("t.closed_at")).isin(1, 3)), "left_anti")
    .filter(F.col("channel") == "phone")
)

sequenced = (
    screened
    .select(
        "id",
        "agent_id",
        "tags",
        F.size(F.collect_set("channel").over(w_agent_id_csat)).alias("distinct_channel_in_agent_id_csat"),
        F.round(F.percent_rank().over(w_all_by_days_since_opened_at), 3).alias("days_since_opened_at_percent_rank"),
        F.sum("csat").over(w_all_by_days_since_opened_at_2).alias("running_csat"),
    )
)

detail_rows = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.tags",
        "s.distinct_channel_in_agent_id_csat",
        "s.days_since_opened_at_percent_rank",
        "s.running_csat",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

with_details = (
    detail_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.agent_name").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.distinct_channel_in_agent_id_csat",
        "s.days_since_opened_at_percent_rank",
        "s.running_csat",
        "s.event_id",
        "s.event_type",
        "s.event_at",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

result = (
    with_details
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "days_since_opened_at_percent_rank",
        "running_csat",
        "event_id",
        "event_type",
        "event_at",
        "agent_team",
        "agent_hired_on",
    )
    .orderBy(
        F.col("days_since_opened_at_percent_rank").desc(),
        F.col("agent_hired_on").asc_nulls_last(),
        "id",
        "event_id",
    )
    .limit(10)
)
