from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.col("csat") >= 1) | (F.col("channel") != "phone"))
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
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

totals = (
    valid
    .groupBy("channel", "csat", "agent_id")
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.sum("requester_id").alias("total_requester_id"),
        F.max("csat").alias("max_csat"),
    )
)

filtered = (
    totals.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isNotNull()), "left_anti")
)

result = (
    filtered
    .select(
        "channel",
        "csat",
        "max_csat",
        "total_requester_id",
    )
    .unpivot(
        ids=["channel", "csat"],
        values=["max_csat", "total_requester_id"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
