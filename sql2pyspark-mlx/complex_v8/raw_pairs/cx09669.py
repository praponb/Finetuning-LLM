from pyspark.sql import functions as F

prepared = df_tickets.filter(F.col("requester_id") <= 23)

joined = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") == "T1")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
    )
)

scored = (
    joined
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        "agent_tier",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
)

rollup_base = (
    scored
    .groupBy("csat", "agent_tier", "channel")
    .agg(
        F.array_sort(F.collect_set("agent_tier")).alias("agent_tier_list"),
        F.count("*").alias("n_rows"),
    )
)

flattened = (
    rollup_base
    .select(
        "csat",
        "agent_tier",
        "channel",
        "n_rows",
        F.posexplode_outer("agent_tier_list").alias("agent_tier_list_item_pos", "agent_tier_list_item"),
    )
)

tagged = (
    flattened
    .filter(F.col("n_rows") <= 11)
    .select(
        "csat",
        "agent_tier",
        "channel",
        "agent_tier_list_item_pos",
        "agent_tier_list_item",
        F.lower(F.regexp_replace(F.trim(F.col("agent_tier")), " ", "_")).alias("agent_tier_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_tier_list_item")), " ", "_")).alias("agent_tier_list_item_slug"),
    )
)

result = (
    tagged
    .filter(F.col("agent_tier_list_item_pos") <= 3)
    .select(
        "csat",
        "agent_tier",
        "agent_tier_list_item_pos",
        "agent_tier_list_item",
        "agent_tier_slug",
        "agent_tier_list_item_slug",
        F.round(F.col("csat") / F.nullif(F.col("agent_tier_list_item_pos"), F.lit(0)), 3).alias("csat_per_agent_tier_list_item_pos"),
        F.concat_ws(" / ", F.col("agent_tier"), F.col("channel")).alias("agent_tier_channel_label"),
    )
    .orderBy(
        F.col("agent_tier_slug").desc(),
        F.col("csat_per_agent_tier_list_item_pos").desc_nulls_first(),
    )
)
