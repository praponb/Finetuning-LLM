from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("channel").orderBy("closed_at_month", "id")

recent_tickets = (
    df_tickets
    .filter((F.col("channel").isNotNull()) & (F.col("agent_id") <= 8))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

labeled = (
    recent_tickets
    .filter(F.col("channel_prefix").isNotNull())
    .select(
        "*",
        F.month(F.col("closed_at")).alias("closed_at_month"),
        F.array_contains(F.col("tags"), "bug").alias("has_bug"),
    )
)

latest_per_key = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

per_group = (
    latest_per_key
    .filter(F.col("closed_at_month") > 2)
    .groupBy("agent_id", "has_bug")
    .agg(
        F.min("closed_at").alias("first_closed_at"),
        F.round(100 * F.count(F.when(F.col("channel") == "web", 1)) / F.count("*"), 1).alias("pct_channel_web"),
        F.max("closed_at").alias("last_closed_at"),
        F.count("*").alias("n_rows"),
        F.sum("closed_at_month").alias("total_closed_at_month"),
        F.avg("closed_at_month").alias("avg_closed_at_month_check"),
    )
    .filter(F.col("avg_closed_at_month_check") > 3)
    .drop("avg_closed_at_month_check")
)

result = (
    per_group
    .select(
        "agent_id",
        "has_bug",
        "total_closed_at_month",
        "n_rows",
    )
    .unpivot(
        ids=["agent_id", "has_bug"],
        values=["total_closed_at_month", "n_rows"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
