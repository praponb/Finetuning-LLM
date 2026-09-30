from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_agent_hired_on_ru = Window.partitionBy("channel").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_2_month_by_ag = Window.partitionBy(F.date_trunc("month", "agent_hired_on_2")).orderBy(F.col("agent_hired_on_2").desc(), "id")
w_channel_by_agent_hired_on_fu = Window.partitionBy("channel").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_by_agent_hired_on = Window.partitionBy("channel").orderBy("agent_hired_on", "id")

cleaned_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 8) & (F.col("csat") < 4))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

with_parent_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.lower(F.col("su.agent_name")).like("% f%")), "inner")
    .filter(F.col("su.hired_on") < F.lit("2025-02-27").cast("date"))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.channel_prefix",
        "s.opened_at_label",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

enriched = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.opened_at_label",
        "s.agent_name",
        "s.agent_hired_on",
        F.col("su.hired_on").alias("agent_hired_on_2"),
        F.col("su.team").alias("agent_team"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "opened_at_label",
        "agent_name",
        "agent_hired_on_2",
        "agent_team",
        F.last("agent_hired_on", ignorenulls=True).over(w_channel_by_agent_hired_on_ru).alias("last_known_agent_hired_on"),
        F.lead("agent_hired_on", 2).over(w_agent_hired_on_2_month_by_ag).alias("next2_agent_hired_on"),
        F.nth_value("agent_hired_on", 2).over(w_channel_by_agent_hired_on_fu).alias("second_agent_hired_on"),
        F.ntile(10).over(w_channel_by_agent_hired_on).alias("agent_hired_on_decile"),
        F.first("closed_at", ignorenulls=True).over(w_channel_by_agent_hired_on_ru).alias("first_known_closed_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at_label",
        "agent_hired_on_2",
        "last_known_agent_hired_on",
        "next2_agent_hired_on",
        "second_agent_hired_on",
        "agent_hired_on_decile",
        "first_known_closed_at",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("first_known_closed_at"))) / 3600, 1).alias("hours_first_known_closed_at_to_closed_at"),
        F.datediff(F.col("second_agent_hired_on"), F.col("agent_hired_on_2")).alias("days_agent_hired_on_2_to_second_agent_hired_on"),
    )
)
