from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("priority", "channel").orderBy("agent_hired_on", "id")
w_median_csat = Window.partitionBy("median_csat")
w_all_by_median_csat = Window.orderBy("median_csat", "agent_hired_on_ym")

prepared = (
    df_tickets
    .filter(F.col("csat") > 3)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

unique_rows = (
    with_parent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

rollup_base = (
    unique_rows
    .filter(F.col("agent_id") >= 7)
    .groupBy(F.date_format("agent_hired_on", "yyyy-MM").alias("agent_hired_on_ym"))
    .agg(
        F.min("csat").alias("min_csat"),
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
    )
)

sequenced = (
    rollup_base
    .select(
        "agent_hired_on_ym",
        "median_csat",
        "avg_csat",
        F.round(F.col("min_csat") / F.nullif(F.sum("min_csat").over(w_median_csat), F.lit(0)), 4).alias("min_csat_share_of_median_csat"),
        F.dense_rank().over(w_all_by_median_csat).alias("median_csat_dense_rank"),
    )
)

tagged = (
    sequenced
    .select(
        "agent_hired_on_ym",
        "avg_csat",
        "min_csat_share_of_median_csat",
        "median_csat_dense_rank",
        (F.col("avg_csat") + F.col("median_csat_dense_rank")).alias("avg_csat_plus_median_csat_dense_rank"),
        F.round(F.col("avg_csat") * 0.9, 2).alias("avg_csat_adj"),
    )
)

result = (
    tagged
    .select(
        "agent_hired_on_ym",
        "avg_csat",
        "median_csat_dense_rank",
        "avg_csat_plus_median_csat_dense_rank",
        "avg_csat_adj",
        F.round(F.col("avg_csat_adj") * 1.5, 2).alias("avg_csat_adj_adj"),
        F.round(F.col("avg_csat_adj") * 0.9, 2).alias("avg_csat_adj_adj_2"),
    )
    .orderBy(F.col("avg_csat").asc(), F.col("avg_csat_adj_adj_2").desc())
)
