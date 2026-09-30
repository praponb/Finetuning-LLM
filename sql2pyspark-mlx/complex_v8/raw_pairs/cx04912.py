from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
        F.when(F.col("requester_id") >= 20, "A").when(F.col("requester_id") >= 7, "B").otherwise("C").alias("requester_id_tier"),
    )
)

derived = (
    prepared_tickets
    .select(
        "*",
        F.when(F.col("requester_id_tier").isin("A", "B"), "group_a").when(F.col("requester_id_tier") == "C", "group_b").otherwise("other").alias("requester_id_tier_group"),
        F.when(F.col("requester_id") >= 18, "HIGH").when(F.col("requester_id") >= 6, "MEDIUM").otherwise("LOW").alias("requester_id_tier_2"),
        F.when(F.col("channel").isin("email", "chat"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
    )
)

result = (
    derived
    .groupingSets(
        [["channel_group", "requester_id_tier"], ["channel_group"], ["requester_id_tier"], []],
        "channel_group", "requester_id_tier",
    )
    .agg(
        F.sum("requester_id").alias("total_requester_id"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
    )
    .select(
        F.coalesce(F.col("channel_group"), F.lit("ALL")).alias("channel_group"),
        F.coalesce(F.col("requester_id_tier"), F.lit("ALL")).alias("requester_id_tier"),
        "total_requester_id",
        "channel_list",
    )
)
