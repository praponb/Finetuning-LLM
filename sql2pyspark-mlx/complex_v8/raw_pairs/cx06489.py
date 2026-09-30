from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

tagged = (
    prepared_tickets
    .select(
        "*",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
        F.coalesce(F.col("days_since_opened_at"), F.lit(0)).alias("days_since_opened_at_filled"),
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
    )
)

screened = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.size(F.col("t.tags")) >= 1) & (F.hour(F.col("t.closed_at")).between(6, 11)), "left_semi")
)

agent_id_stats = (
    screened
    .groupBy("agent_id")
    .agg(F.avg("days_since_opened_at_filled").alias("avg_days_since_opened_at_filled"))
)

outliers = (
    screened.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.days_since_opened_at_filled") > F.col("g.avg_days_since_opened_at_filled"))
    .select(
        "s.*",
        "g.avg_days_since_opened_at_filled",
        F.round(F.col("s.days_since_opened_at_filled") - F.col("g.avg_days_since_opened_at_filled"), 2).alias("days_since_opened_at_filled_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "avg_days_since_opened_at_filled",
        "days_since_opened_at_filled_minus_avg",
    )
    .unpivot(
        ids=["id"],
        values=["avg_days_since_opened_at_filled", "days_since_opened_at_filled_minus_avg"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .orderBy("id", "metric_name")
)
