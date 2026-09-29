from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-01-27 00:00:00").cast("timestamp")) & (F.hour(F.col("opened_at")).between(9, 14)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
        F.when(F.col("channel").isin("chat", "email"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "replied"), "left_anti")
    .filter(F.col("channel_group") == "group_b")
)

rollup_base = (
    with_activity
    .groupBy("agent_id")
    .agg(
        F.round(F.sum("greatest_requester_id_csat") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("greatest_requester_id_csat_to_csat_ratio"),
        F.min("requester_id").alias("min_requester_id"),
        F.sum("greatest_requester_id_csat").alias("sum_greatest_requester_id_csat_check"),
    )
    .filter(F.col("sum_greatest_requester_id_csat_check") > 7)
    .drop("sum_greatest_requester_id_csat_check")
)

result = (
    rollup_base
    .filter(F.col("min_requester_id") > 11)
    .select(
        "agent_id",
        "greatest_requester_id_csat_to_csat_ratio",
        "min_requester_id",
    )
)
