from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel_by_min_tick = Window.partitionBy("agent_id", "channel").orderBy("min_ticket_csat", "id")
w_agent_id = Window.partitionBy("agent_id")
w_min_ticket_csat_by_min_ticke = Window.partitionBy("min_ticket_csat").orderBy(F.col("min_ticket_csat").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_min_ticket_csat_by_min_ticke_2 = Window.partitionBy("min_ticket_csat").orderBy(F.col("min_ticket_csat").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("requester_id") <= 14))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

combined_details = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("J%")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat_minus_requester_id",
        F.col("su.tier").alias("agent_tier"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.min("csat").alias("min_ticket_csat"))
)

profiled = (
    combined_details.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.min_ticket_csat")
)

windowed = (
    profiled
    .select(
        "*",
        F.ntile(5).over(w_agent_id_channel_by_min_tick).alias("min_ticket_csat_quintile"),
        F.min("csat_minus_requester_id").over(w_agent_id).alias("agent_id_min_csat_minus_requester_id"),
        F.round(F.col("csat_minus_requester_id") / F.nullif(F.sum("csat_minus_requester_id").over(w_agent_id), F.lit(0)), 4).alias("csat_minus_requester_id_share_of_agent_id"),
        F.round(F.avg("min_ticket_csat").over(w_min_ticket_csat_by_min_ticke), 2).alias("min_ticket_csat_avg_3"),
        F.max("min_ticket_csat").over(w_min_ticket_csat_by_min_ticke_2).alias("min_ticket_csat_max_4"),
        F.rank().over(w_agent_id_channel_by_min_tick).alias("min_ticket_csat_rank"),
    )
)

result = (
    windowed
    .groupingSets(
        [["agent_tier", "min_ticket_csat_rank"], ["agent_tier"], ["min_ticket_csat_rank"], []],
        "agent_tier", "min_ticket_csat_rank",
    )
    .agg(
        F.grouping("agent_tier").alias("is_total_agent_tier"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
    )
)
