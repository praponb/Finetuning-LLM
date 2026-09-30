from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

derived = (
    scoped_tickets
    .withColumn("priority_group", F.when(F.col("priority").isin("P4", "P3"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other"))
)

result = (
    derived
    .groupingSets(
        [["priority", "priority_group", "agent_id"], ["priority"], ["priority_group"], []],
        "priority", "priority_group", "agent_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("requester_id_minus_csat") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("requester_id_minus_csat_to_csat_ratio"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        F.coalesce(F.col("priority_group"), F.lit("ALL")).alias("priority_group"),
        "agent_id",
        "gid",
        "requester_id_minus_csat_to_csat_ratio",
        "avg_csat",
        "priority_list",
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("priority_group").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
