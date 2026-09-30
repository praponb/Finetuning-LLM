from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter((F.col("channel") == "email") & (F.to_date(F.col("opened_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
        F.when(F.col("requester_id") >= 21, "A").when(F.col("requester_id") >= 8, "B").otherwise("C").alias("requester_id_tier"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.abs(F.col("requester_id") - 7).alias("requester_id_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.month(F.col("closed_at")).alias("closed_at_month"),
    )
)

stats = (
    scored
    .groupBy(
        "agent_id",
        F.when(F.col("requester_id_per_csat") >= 5.5, "large").when(F.col("requester_id_per_csat") >= 3.0, "medium").otherwise("small").alias("requester_id_per_csat_tier"),
    )
    .agg(
        F.max("closed_at_month").alias("max_closed_at_month"),
        F.sum(F.when(F.col("channel") == "email", 1).otherwise(0)).alias("n_channel_email"),
    )
)

result = (
    stats
    .groupBy("agent_id")
    .pivot("requester_id_per_csat_tier", ["small", "medium"])
    .agg(F.sum("max_closed_at_month"))
    .orderBy("agent_id")
)
