from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_priority_by_closed_at = Window.partitionBy("priority").orderBy(F.col("closed_at").desc(), "id")
w_csat_agent_id_by_requester_i = Window.partitionBy("csat", "agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_agent_id_by_requester_i_2 = Window.partitionBy("csat", "agent_id").orderBy("requester_id", "id")

scoped_tickets = (
    df_tickets
    .filter(F.col("priority") != "P2")
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
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
    )
)

with_windows = (
    scoped_tickets
    .select(
        "*",
        F.round(F.avg("requester_id").over(w_agent_id), 2).alias("agent_id_avg_requester_id"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id), 2).alias("csat_vs_agent_id_avg"),
        F.row_number().over(w_priority_by_closed_at).alias("seq_no"),
        F.sum("csat").over(w_csat_agent_id_by_requester_i).alias("running_csat"),
        F.max("csat").over(w_agent_id).alias("agent_id_max_csat"),
        F.lag("csat", 3).over(w_csat_agent_id_by_requester_i_2).alias("prev3_csat"),
    )
)

combined_details = (
    with_windows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

result = (
    combined_details
    .groupingSets(
        [["agent_id", "priority", "agent_tier"], ["agent_id"], ["priority"]],
        "agent_id", "priority", "agent_tier",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("channel") == "phone", 1).otherwise(0)).alias("n_channel_phone"),
        F.max("agent_id_max_csat").alias("max_agent_id_max_csat"),
    )
    .select(
        "agent_id",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "gid",
        "n_channel_phone",
        "max_agent_id_max_csat",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
