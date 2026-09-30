from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_days_since_closed = Window.partitionBy("channel").orderBy(F.col("days_since_closed_at_to_opened_at_quarter_ratio").desc(), "agent_id", "channel")
w_rn_channel = Window.partitionBy("rn", "channel")
w_agent_id_by_days_since_close = Window.partitionBy("agent_id").orderBy("days_since_closed_at_to_opened_at_quarter_ratio", "agent_id", "channel")

cleaned_tickets = (
    df_tickets
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

summary = (
    cleaned_tickets
    .filter(F.month(F.col("opened_at")).isin(2, 3))
    .groupBy("agent_id", "channel")
    .agg(
        F.min("closed_at").alias("first_closed_at"),
        F.max("minutes_opened_at_to_closed_at").alias("max_minutes_opened_at_to_closed_at"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.round(F.sum("days_since_closed_at") / F.nullif(F.sum("opened_at_quarter"), F.lit(0)), 3).alias("days_since_closed_at_to_opened_at_quarter_ratio"),
    )
)

enriched = (
    summary.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.channel",
        "s.first_closed_at",
        "s.max_minutes_opened_at_to_closed_at",
        "s.days_since_closed_at_to_opened_at_quarter_ratio",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    enriched
    .select(
        "agent_id",
        "channel",
        "first_closed_at",
        "days_since_closed_at_to_opened_at_quarter_ratio",
        F.row_number().over(w_channel_by_days_since_closed).alias("rn"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority") != "P1"), "left_anti")
    .filter(F.col("agent_id") <= 9)
)

with_windows = (
    without_matches
    .select(
        "*",
        F.round(F.avg("rn").over(w_rn_channel), 2).alias("rn_channel_avg_rn"),
        F.min("rn").over(w_rn_channel).alias("rn_channel_min_rn"),
        F.rank().over(w_agent_id_by_days_since_close).alias("days_since_closed_at_to_opened_at_quarter_ratio_"),
        F.size(F.collect_set("agent_id").over(w_rn_channel)).alias("distinct_agent_id_in_rn_channel"),
    )
)

screened = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.array_contains(F.col("t.tags"), "bug")), "left_semi")
    .filter(F.col("rn") <= 9)
)

result = (
    screened
    .select("rn_channel_avg_rn", "first_closed_at")
    .distinct()
    .orderBy(
        F.col("first_closed_at").desc(),
        F.col("rn_channel_avg_rn").desc_nulls_first(),
    )
)
