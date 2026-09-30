from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_csat_agent_id_by_closed_at_r = Window.partitionBy("csat", "agent_id").orderBy(F.col("closed_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

screened = (
    recent
    .crossJoin(recent.agg(F.min("requester_id").alias("overall_min_requester_id")))
    .filter(F.col("requester_id") > F.col("overall_min_requester_id"))
    .drop("overall_min_requester_id")
)

with_windows = (
    screened
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat), 2).alias("csat_vs_csat_avg"),
        F.min("requester_id_filled").over(w_csat_agent_id_by_closed_at_r).alias("min_requester_id_filled_so_far"),
        F.min("requester_id").over(w_csat).alias("csat_min_requester_id"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["csat", "priority", "agent_id"], ["csat"], ["priority"], []],
        "csat", "priority", "agent_id",
    )
    .agg(
        F.count(F.when(F.col("priority") == "P2", 1)).alias("n_priority_p2"),
        F.min("csat_share_of_csat").alias("min_csat_share_of_csat"),
        F.round(F.avg("min_requester_id_filled_so_far"), 2).alias("avg_min_requester_id_filled_so_far"),
    )
    .orderBy(
        F.col("csat").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
