from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
    )
)

result = (
    prepared
    .filter(~F.col("priority").isin("P1", "P3"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        F.when(F.col("priority").isin("P4", "P1"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
    )
    .orderBy(F.col("priority").desc(), F.col("csat").asc_nulls_last())
)
