from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(~F.col("channel").isin("chat", "phone"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.size(F.col("t.tags")) >= 1), "left_semi")
)

result = (
    with_activity
    .groupingSets(
        [["priority_or_unknown", "agent_id", "priority"], ["priority_or_unknown"], ["agent_id"], []],
        "priority_or_unknown", "agent_id", "priority",
    )
    .agg(F.min("csat").alias("min_csat"), F.count("*").alias("cnt"))
    .select(
        F.coalesce(F.col("priority_or_unknown"), F.lit("ALL")).alias("priority_or_unknown"),
        "agent_id",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "min_csat",
        "cnt",
    )
)
