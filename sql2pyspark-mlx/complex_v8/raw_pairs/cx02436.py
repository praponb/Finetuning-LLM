from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter((F.to_date(F.col("opened_at")) < F.lit("2025-03-08").cast("date")) | (F.col("priority") == "P4"))
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
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_details = (
    joined.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") != "T3")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.opened_at_label",
        "s.closed_at_year",
        "s.agent_name",
        "s.agent_hired_on",
        F.col("su.tier").alias("agent_tier_2"),
    )
)

totals = (
    with_details
    .groupBy(F.date_trunc("month", "agent_hired_on").alias("agent_hired_on_month"))
    .agg(
        F.round(F.stddev("requester_id"), 2).alias("stddev_requester_id"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.array_sort(F.collect_set("agent_tier_2")).alias("agent_tier_2_list"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("n_channel_web"),
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("agent_name"))) >= 8, 1)) / F.count("*"), 1).alias("pct_length_trim_agent_name_8"),
        F.max("requester_id").alias("max_requester_id"),
        F.sum("requester_id").alias("sum_requester_id_check"),
    )
    .filter(F.col("sum_requester_id_check") > 7)
    .drop("sum_requester_id_check")
)

derived = (
    totals
    .filter(F.size(F.col("agent_tier_2_list")) >= 1)
    .withColumn("n_channel_web_per_max_requester_id", F.round(F.col("n_channel_web") / F.nullif(F.col("max_requester_id"), F.lit(0)), 3))
)

result = (
    derived
    .select(
        "agent_hired_on_month",
        "stddev_requester_id",
        "agent_name_list",
        "agent_tier_2_list",
        "n_channel_web",
        "pct_length_trim_agent_name_8",
        "max_requester_id",
        "n_channel_web_per_max_requester_id",
        F.when(F.col("max_requester_id") >= 15, "HIGH").when(F.col("max_requester_id") >= 12, "MEDIUM").otherwise("LOW").alias("max_requester_id_tier"),
        F.month(F.col("agent_hired_on_month")).alias("agent_hired_on_month_month"),
    )
    .orderBy(
        F.col("max_requester_id_tier").desc(),
        "n_channel_web_per_max_requester_id",
        "agent_hired_on_month",
    )
    .limit(5)
)
