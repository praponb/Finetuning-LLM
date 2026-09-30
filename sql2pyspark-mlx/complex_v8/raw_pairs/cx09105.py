from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id_by_requester_i = Window.partitionBy("csat", "agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_csat_agent_id_by_requester_i_2 = Window.partitionBy("csat", "agent_id").orderBy(F.col("requester_id").asc_nulls_last())
w_csat_priority = Window.partitionBy("csat", "priority")
w_csat_agent_id_by_csat_moving = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(-3, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

ranked = (
    filtered_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        F.row_number().over(w_csat_agent_id_by_requester_i).alias("seq_no"),
        F.row_number().over(w_agent_id_by_closed_at).alias("seq_no_2"),
        F.rank().over(w_csat_agent_id_by_requester_i_2).alias("requester_id_rank"),
        F.size(F.collect_set("agent_id").over(w_csat_priority)).alias("distinct_agent_id_in_csat_priority"),
        F.round(F.avg("requester_id").over(w_csat_agent_id_by_csat_moving), 2).alias("requester_id_avg_4"),
    )
)

enriched = (
    ranked.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    enriched.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

result = (
    summarized_rows
    .select(
        "id",
        F.col("seq_no_2").alias("seq_no_2_v"),
        F.col("requester_id_rank").alias("requester_id_rank_v"),
        F.col("distinct_agent_id_in_csat_priority").alias("m_distinct_agent_id_in_csat_priority"),
    )
    .unpivot(
        ids=["id"],
        values=["seq_no_2_v", "requester_id_rank_v", "m_distinct_agent_id_in_csat_priority"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
