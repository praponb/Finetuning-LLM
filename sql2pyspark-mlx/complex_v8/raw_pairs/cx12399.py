from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_csat_running = Window.partitionBy("agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy("csat", "id")
w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_dedup = Window.partitionBy("priority").orderBy(F.col("last_known_opened_at").desc(), F.col("id").desc())

recent = (
    df_tickets
    .filter((F.to_date(F.col("opened_at")) < F.lit("2025-02-12").cast("date")) & (F.col("requester_id") >= 8))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
    )
)

sequenced = (
    recent
    .select(
        "id",
        "priority",
        "csat",
        F.last("opened_at", ignorenulls=True).over(w_agent_id_by_csat_running).alias("last_known_opened_at"),
        F.rank().over(w_agent_id_by_csat).alias("csat_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_agent_id), 2).alias("csat_vs_csat_agent_id_avg"),
    )
)

latest = (
    sequenced
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "priority",
        "csat",
        "csat_rank",
        "csat_vs_csat_agent_id_avg",
        F.date_format(F.col("last_known_opened_at"), "yyyy-MM-dd HH:00").alias("last_known_opened_at_label"),
        F.abs(F.col("csat_rank") - 7).alias("csat_rank_distance"),
    )
    .orderBy(F.col("csat_rank").desc())
)
