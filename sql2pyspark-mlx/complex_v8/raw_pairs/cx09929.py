from pyspark.sql import functions as F

in_window = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.requester_id").alias("prior_requester_id_3d"),
    )
)

qualified = (
    in_window.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

combined_details_agent = (
    qualified.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.prior_rows_3d",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

scored = (
    combined_details_agent
    .select(
        "id",
        "agent_id",
        "prior_rows_3d",
        "agent_tier",
        F.concat(F.col("agent_name"), F.lit("#"), F.col("id").cast("string")).alias("agent_name_ref"),
        F.when(F.col("agent_tier").isin("T3", "T1"), "group_a").when(F.col("agent_tier") == "T2", "group_b").otherwise("other").alias("agent_tier_group"),
    )
)

result = (
    scored
    .select(
        "id",
        "agent_id",
        "prior_rows_3d",
        "agent_tier",
        "agent_tier_group",
        F.lower(F.regexp_replace(F.trim(F.col("agent_tier_group")), " ", "_")).alias("agent_tier_group_slug"),
    )
    .orderBy(
        F.col("prior_rows_3d").desc(),
        F.col("agent_tier_group_slug").desc(),
        "id",
    )
    .limit(20)
)
