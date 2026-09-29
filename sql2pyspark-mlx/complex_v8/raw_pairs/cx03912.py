from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "bug"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

without_matches = (
    prepared_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.hour(F.col("t.opened_at")).between(6, 11)) & (F.hour(F.col("t.closed_at")).between(6, 11)), "left_anti")
)

result = (
    without_matches
    .groupingSets(
        [["priority", "agent_id", "channel"], ["priority"]],
        "priority", "agent_id", "channel",
    )
    .agg(F.count(F.when(F.col("priority") == "P4", 1)).alias("n_priority_p4"))
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
    )
)
