from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id", "channel").orderBy(F.col("closed_at_plus_30m").desc(), F.col("id").desc())

scoped = (
    df_tickets
    .filter((F.col("channel") == "phone") & (F.col("csat").between(2, 4)))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        (F.col("closed_at") + F.expr("INTERVAL 30 MINUTES")).alias("closed_at_plus_30m"),
    )
)

deduped = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

agent_id_stats = (
    deduped
    .groupBy("agent_id")
    .agg(F.avg("csat").alias("avg_csat"))
)

outliers = (
    deduped.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

result = (
    outliers
    .select("id", "avg_csat", "csat_minus_avg")
    .unpivot(
        ids=["id"],
        values=["avg_csat", "csat_minus_avg"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
