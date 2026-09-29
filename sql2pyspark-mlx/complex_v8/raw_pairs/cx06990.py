from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy("days_since_closed_at", "id")

recent = (
    df_tickets
    .filter(F.col("requester_id") >= 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

unique_rows = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .groupBy("agent_id")
    .pivot("channel", ["chat", "phone"])
    .agg(F.sum("days_since_closed_at"))
    .orderBy("agent_id")
)
