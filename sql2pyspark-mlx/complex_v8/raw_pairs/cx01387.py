from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_channel = Window.partitionBy("agent_id", "channel")

cleaned_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.date_format(F.col("closed_at"), "MMM yyyy").alias("closed_at_label"),
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_parent_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    with_parent_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "closed_at_label",
        "channel_priority_label",
        "agent_tier",
        "agent_hired_on",
        F.last("closed_at").over(w_all_by_closed_at_full).alias("last_closed_at"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_channel), F.lit(0)), 4).alias("requester_id_share_of_agent_id_channel"),
    )
)

screened = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.closed_at") < F.lit("2025-02-27 06:00:00").cast("timestamp")), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["priority", "agent_tier"], ["priority"], []],
        "priority", "agent_tier",
    )
    .agg(
        F.max("agent_hired_on").alias("last_agent_hired_on"),
        F.max("requester_id").alias("max_requester_id"),
        F.count("*").alias("n_rows"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "last_agent_hired_on",
        "max_requester_id",
        "n_rows",
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
