from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_filled_channel = Window.partitionBy("csat_filled", "channel")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_agent_id_by_requester_id_run = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat_filled_ties = Window.orderBy(F.col("csat_filled").desc())
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy(F.col("csat").desc(), "id")

filtered = (
    df_tickets
    .filter((F.col("channel") == "chat") & (F.col("closed_at") >= F.lit("2025-02-04 23:00:00").cast("timestamp")))
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
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "csat_filled",
        "priority_clean",
        F.round(F.avg("csat_filled").over(w_csat_filled_channel), 2).alias("csat_filled_channel_avg_csat_filled"),
        F.round(F.col("csat_filled") - F.avg("csat_filled").over(w_agent_id_csat), 2).alias("csat_filled_vs_agent_id_csat_avg"),
        F.sum("csat").over(w_agent_id_by_requester_id_run).alias("running_csat"),
        F.size(F.collect_set("channel").over(w_agent_id_csat)).alias("distinct_channel_in_agent_id_csat"),
        F.count(F.lit(1)).over(w_csat_filled_channel).alias("rows_in_csat_filled_channel"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_csat), 2).alias("csat_vs_agent_id_csat_avg"),
        F.dense_rank().over(w_all_by_csat_filled_ties).alias("csat_filled_dense_rank"),
        F.row_number().over(w_agent_id_by_csat).alias("rn"),
        F.sum("csat_filled").over(w_agent_id_by_requester_id_run).alias("running_csat_filled"),
        F.round(F.percent_rank().over(w_agent_id_by_csat), 3).alias("csat_percent_rank"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.round(F.col("csat_filled") / F.nullif(F.col("csat_percent_rank"), F.lit(0)), 3).alias("csat_filled_per_csat_percent_rank"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

result = (
    labeled
    .filter(F.col("csat_filled_per_csat_percent_rank") < 8.5)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "priority_clean",
        "csat_filled_vs_agent_id_csat_avg",
        "running_csat",
        "distinct_channel_in_agent_id_csat",
        "rows_in_csat_filled_channel",
        "rn",
        "running_csat_filled",
    )
    .orderBy(F.col("id").asc())
    .limit(10)
)
