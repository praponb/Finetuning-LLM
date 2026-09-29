from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd HH:00").alias("opened_at_label"),
        F.date_format(F.col("closed_at"), "MMM yyyy").alias("closed_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

enriched = (
    prepared
    .filter(F.col("opened_at_label").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        "opened_at_label",
        "closed_at_label",
        F.when(F.col("priority").isin("P2", "P1"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
    )
)

priority_stats = (
    enriched
    .groupBy("priority")
    .agg(F.percentile_approx("requester_id", 0.5).alias("median_requester_id"))
)

vs_group = (
    enriched.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.requester_id") >= F.col("g.median_requester_id"))
    .select(
        "s.*",
        "g.median_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.median_requester_id"), 2).alias("requester_id_minus_median"),
    )
)

filtered = (
    vs_group.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

scored = (
    filtered
    .filter(F.col("opened_at_label").isNotNull())
    .select(
        "id",
        "agent_id",
        "priority",
        "closed_at_label",
        "median_requester_id",
        "requester_id_minus_median",
        F.concat(F.col("opened_at_label"), F.lit("#"), F.col("agent_id").cast("string")).alias("opened_at_label_ref"),
    )
)

derived = (
    scored
    .select(
        "id",
        "agent_id",
        "priority",
        "closed_at_label",
        "median_requester_id",
        "requester_id_minus_median",
        "opened_at_label_ref",
        F.lower(F.regexp_replace(F.trim(F.col("closed_at_label")), " ", "_")).alias("closed_at_label_slug"),
        (F.floor(F.col("median_requester_id") / 5) * 5).alias("median_requester_id_bucket"),
        F.concat_ws(": ", F.col("closed_at_label"), F.col("priority")).alias("closed_at_label_priority_label"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("G%"))
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.median_requester_id",
        "s.opened_at_label_ref",
        "s.closed_at_label_slug",
        "s.median_requester_id_bucket",
        "s.closed_at_label_priority_label",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    combined_details
    .filter(F.col("agent_name").isNotNull())
    .select(
        "id",
        "agent_id",
        "priority",
        "opened_at_label_ref",
        "closed_at_label_slug",
        "median_requester_id_bucket",
        "agent_name",
        "agent_tier",
        F.date_format(F.col("agent_hired_on"), "yyyy-MM").alias("agent_hired_on_label"),
    )
    .orderBy(F.col("agent_name").desc())
)
