from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_team").orderBy("closed_at", "id")

filtered_tickets = (
    df_tickets
    .filter(F.col("requester_id") >= 5)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.concat_ws(": ", F.col("channel"), F.col("priority")).alias("channel_priority_label_2"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

joined = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.channel_priority_label",
        "s.channel_priority_label_2",
        "s.requester_id_bucket",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

enriched = (
    joined.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.channel_priority_label_2",
        "s.requester_id_bucket",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_details_agent = (
    enriched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"))
)

deduped = (
    with_details_agent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "tags",
        "channel_priority_label_2",
        "requester_id_bucket",
        "agent_tier",
        "agent_team",
        "agent_hired_on",
        F.abs(F.col("requester_id_bucket") - 15).alias("requester_id_bucket_distance"),
        F.when(F.col("agent_tier").isin("T3", "T2"), "group_a").when(F.col("agent_tier") == "T1", "group_b").otherwise("other").alias("agent_tier_group"),
    )
)
