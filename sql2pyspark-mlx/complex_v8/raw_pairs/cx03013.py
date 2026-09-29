from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (~F.col("priority").isin("P4", "P2")))
)

tagged = (
    filtered_tickets
    .select(
        "*",
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
    )
)

result = (
    tagged
    .groupingSets(
        [["channel", "agent_id"], ["channel"], ["agent_id"], []],
        "channel", "agent_id",
    )
    .agg(
        F.min("csat").alias("min_csat"),
        F.sum("requester_id_minus_csat").alias("total_requester_id_minus_csat"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("channel").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
