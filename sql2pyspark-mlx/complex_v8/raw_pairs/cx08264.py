from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_2 = Window.partitionBy("ticket_channel_2")
w_ticket_id_2_by_ticket_reques = Window.partitionBy("ticket_id_2").orderBy(F.col("ticket_requester_id").desc(), "id", "ticket_id", "ticket_id_2").rowsBetween(-2, Window.currentRow)

valid = (
    df_support_agents
    .filter((F.col("team").isin("VIP", "Tech")) & (F.upper(F.trim(F.col("agent_name"))).like("H%")))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.quarter(F.col("hired_on")).alias("hired_on_quarter"),
        F.concat_ws(": ", F.col("tier"), F.col("agent_name")).alias("tier_agent_name_label"),
        F.upper(F.trim(F.col("team"))).alias("team_clean"),
    )
)

tagged = (
    valid
    .filter(F.col("tier_agent_name_label").isNotNull())
    .select(
        "*",
        F.quarter(F.col("hired_on")).alias("hired_on_quarter_2"),
        F.lower(F.regexp_replace(F.trim(F.col("team_clean")), " ", "_")).alias("team_clean_slug"),
    )
)

exploded_detail = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.team",
        "s.tier_agent_name_label",
        "s.hired_on_quarter_2",
        "s.team_clean_slug",
        F.col("t.id").alias("ticket_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

line_level = (
    exploded_detail.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.team",
        "s.hired_on_quarter_2",
        "s.team_clean_slug",
        "s.ticket_id",
        "s.ticket_opened_at",
        "s.ticket_csat",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.channel").alias("ticket_channel_2"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

with_windows = (
    line_level
    .withColumn("hired_on_quarter_2_share_of_ticket_channel_2", F.round(F.col("hired_on_quarter_2") / F.nullif(F.sum("hired_on_quarter_2").over(w_ticket_channel_2), F.lit(0)), 4))
    .withColumn("hired_on_quarter_2_sum_3", F.sum("hired_on_quarter_2").over(w_ticket_id_2_by_ticket_reques))
)

with_details_ticket = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.team",
        "s.hired_on_quarter_2",
        "s.team_clean_slug",
        "s.ticket_id",
        "s.ticket_opened_at",
        "s.ticket_id_2",
        "s.ticket_channel_2",
        "s.ticket_requester_id",
        "s.hired_on_quarter_2_share_of_ticket_channel_2",
        "s.hired_on_quarter_2_sum_3",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.agent_name").alias("agent_agent_name"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_details_ticket_2 = (
    with_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.team",
        "s.hired_on_quarter_2",
        "s.ticket_id",
        "s.ticket_opened_at",
        "s.ticket_id_2",
        "s.ticket_channel_2",
        "s.ticket_requester_id",
        "s.hired_on_quarter_2_share_of_ticket_channel_2",
        "s.hired_on_quarter_2_sum_3",
        "s.ticket_channel",
        "s.ticket_priority",
        "s.agent_agent_name",
        "s.agent_hired_on",
        F.col("t.channel").alias("ticket_channel_3"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    with_details_ticket_2
    .filter(F.col("hired_on_quarter_2_sum_3") <= 9)
    .select(
        "id",
        "team",
        "hired_on_quarter_2",
        "ticket_opened_at",
        "ticket_id_2",
        "ticket_channel_2",
        "ticket_requester_id",
        "hired_on_quarter_2_share_of_ticket_channel_2",
        "ticket_channel",
        "ticket_priority",
        "agent_agent_name",
        "agent_hired_on",
        "ticket_channel_3",
        F.coalesce(F.col("ticket_channel_2"), F.lit("UNKNOWN")).alias("ticket_channel_2_or_unknown"),
    )
)
