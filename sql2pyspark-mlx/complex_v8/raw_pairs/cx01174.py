from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("requester_id").between(4, 17))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

annotated = (
    valid
    .filter(F.col("agent_id").isNotNull())
    .select(
        "*",
        F.date_trunc("hour", F.col("closed_at")).alias("closed_at_hour"),
        F.concat(F.col("channel"), F.lit("#"), F.col("csat_filled").cast("string")).alias("channel_ref"),
        F.round(F.col("requester_id_per_csat") / F.nullif(F.col("csat_filled"), F.lit(0)), 3).alias("requester_id_per_csat_per_csat_filled"),
    )
)

filtered = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (~F.array_contains(F.col("t.tags"), "outage")) & (F.col("t.channel").isin("email", "phone")), "left_semi")
)

grouped = (
    filtered
    .groupBy("csat_filled", "channel", F.to_date("closed_at").alias("closed_at_day"))
    .agg(
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    grouped
    .select(
        "csat_filled",
        "channel",
        "distinct_agent_ids",
        "n_rows",
    )
    .unpivot(
        ids=["csat_filled", "channel"],
        values=["distinct_agent_ids", "n_rows"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
)
