from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

result = (
    cleaned_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "days_since_opened_at",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
    .orderBy(
        F.col("days_since_opened_at").desc_nulls_first(),
        F.col("id").asc_nulls_last(),
    )
)
