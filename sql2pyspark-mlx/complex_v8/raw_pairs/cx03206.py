from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter(F.col("priority").isin("P1", "P4"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "tags",
    )
)

result = (
    base_tickets
    .rollup("priority", "agent_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.max("requester_id").alias("max_requester_id"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "agent_id",
        "grouping_level",
        "distinct_prioritys",
        "channel_list",
        "max_requester_id",
    )
    .orderBy(F.col("priority").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
