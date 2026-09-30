from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("csat_plus_requester_id").desc(), F.col("id").desc())

valid_tickets = (
    df_tickets
    .filter((F.to_date(F.col("closed_at")) < F.lit("2025-02-10").cast("date")) & (F.col("agent_id") >= 9))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

deduped = (
    valid_tickets
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .rollup("csat", "agent_id")
    .agg(
        F.grouping("csat").alias("is_total_csat"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("csat_plus_requester_id"), 2).alias("avg_csat_plus_requester_id"),
        F.max("closed_at").alias("last_closed_at"),
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
