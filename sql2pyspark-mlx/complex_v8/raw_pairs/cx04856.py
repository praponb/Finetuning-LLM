from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-14 22:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.abs(F.col("csat") - 2).alias("csat_distance"),
        F.when(F.col("csat") >= 3, "large").when(F.col("csat") >= 2, "medium").otherwise("small").alias("csat_tier"),
        F.abs(F.col("csat") - 4).alias("csat_distance_2"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.hour(F.col("opened_at")).alias("opened_at_hour"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.coalesce(F.col("csat_distance"), F.lit(0)).alias("csat_distance_filled"),
    )
)

result = (
    scored
    .select(
        "id",
        "opened_at_hour",
        "csat",
        "csat_distance",
        "csat_distance_filled",
    )
    .unpivot(
        ids=["id"],
        values=["opened_at_hour", "csat", "csat_distance", "csat_distance_filled"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
