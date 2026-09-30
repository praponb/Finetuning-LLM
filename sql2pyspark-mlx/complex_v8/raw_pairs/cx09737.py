from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(6, 11)) & (F.month(F.col("opened_at")).isin(1, 2)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

scored = (
    filtered_tickets
    .select(
        "*",
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
        F.abs(F.col("requester_id") - 9).alias("requester_id_distance"),
        F.lower(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

result = (
    scored
    .rollup("priority", "agent_id")
    .agg(
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.sum("greatest_requester_id_csat").alias("total_greatest_requester_id_csat"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("priority").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
