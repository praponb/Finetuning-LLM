from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id", "priority").orderBy("closed_at", "id")

scoped_tickets = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-01-19 10:00:00").cast("timestamp")) & (~F.col("priority").isin("P3", "P1")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

deduped = (
    scoped_tickets
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupingSets(
        [["csat", "priority"], ["csat"], ["priority"], []],
        "csat", "priority",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.max("closed_at").alias("last_closed_at"),
    )
    .select(
        "csat",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "gid",
        "last_closed_at",
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("priority").asc_nulls_last())
)
