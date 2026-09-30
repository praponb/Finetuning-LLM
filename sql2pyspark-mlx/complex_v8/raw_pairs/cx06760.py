from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter((F.col("requester_id") <= 17) & (F.col("channel").isin("email", "phone", "chat")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
    )
)

qualified = (
    base_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") < 4) & (F.col("t.priority").isin("P4", "P1")), "left_semi")
)

per_group = (
    qualified
    .filter(F.col("csat").between(3, 4))
    .groupBy("agent_id", "channel")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("csat").alias("total_csat"),
        F.sum("requester_id").alias("total_requester_id"),
        F.sum(F.when(~F.col("priority").isin("P2", "P3"), F.col("csat")).otherwise(0)).alias("csat_priority_not_in_p2_p3"),
        F.max("csat_minus_requester_id").alias("max_csat_minus_requester_id"),
        F.sum(F.when(F.col("channel").isNotNull(), 1).otherwise(0)).alias("n_channel_is_not_null"),
    )
)

result = (
    per_group
    .rollup("agent_id", "distinct_ids")
    .agg(
        F.sum("max_csat_minus_requester_id").alias("total_max_csat_minus_requester_id"),
        F.sum("distinct_ids").alias("total_distinct_ids"),
    )
)
