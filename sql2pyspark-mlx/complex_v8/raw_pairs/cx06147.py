from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter(F.col("priority").isin("P4", "P1", "P2"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
    )
)

result = (
    recent_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled_2"),
    )
    .orderBy(
        F.col("requester_id").asc_nulls_last(),
        F.col("csat_filled_2").desc_nulls_first(),
    )
)
