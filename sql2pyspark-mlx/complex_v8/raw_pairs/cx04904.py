from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_latest = Window.partitionBy("priority").orderBy(F.col("distinct_priority_in_channel").desc(), F.col("id").desc())
w_agent_team_by_csat_running = Window.partitionBy("agent_team").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_requester_id_per = Window.partitionBy("priority").orderBy(F.col("requester_id_per_csat").desc(), "id")
w_priority = Window.partitionBy("priority")
w_agent_team_by_channel_min_cs = Window.partitionBy("agent_team").orderBy(F.col("channel_min_csat").desc(), "id")

filtered_tickets = (
    df_tickets
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
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

with_windows = (
    filtered_tickets
    .select(
        "*",
        F.size(F.collect_set("priority").over(w_channel)).alias("distinct_priority_in_channel"),
        F.min("requester_id").over(w_channel).alias("channel_min_requester_id"),
        F.min("csat").over(w_channel).alias("channel_min_csat"),
    )
)

enriched = (
    with_windows
    .filter(F.col("closed_at") < F.lit("2025-02-20 15:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.greatest(F.col("channel_min_requester_id"), F.col("requester_id_per_csat")).alias("greatest_channel_min_requester_id_requester_id_p"),
    )
)

unique_rows = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details = (
    unique_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.requester_id_per_csat",
        "s.channel_min_requester_id",
        "s.channel_min_csat",
        "s.opened_at_label",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

with_windows_2 = (
    with_details
    .select(
        "*",
        F.last("csat", ignorenulls=True).over(w_agent_team_by_csat_running).alias("last_known_csat"),
        F.lead("requester_id").over(w_priority_by_requester_id_per).alias("next_requester_id"),
        F.size(F.collect_set("agent_team").over(w_priority)).alias("distinct_agent_team_in_priority"),
        F.lead("channel_min_csat", 1, 0).over(w_agent_team_by_channel_min_cs).alias("next_channel_min_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_priority), F.lit(0)), 4).alias("csat_share_of_priority"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "requester_id_per_csat",
        "channel_min_requester_id",
        "channel_min_csat",
        "agent_team",
        "last_known_csat",
        "next_requester_id",
        "distinct_agent_team_in_priority",
        "next_channel_min_csat",
        "csat_share_of_priority",
        "distinct_agent_id_in_priority",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("agent_id").desc_nulls_first())
    .limit(10)
)
