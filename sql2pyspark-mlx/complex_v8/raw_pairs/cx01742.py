from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_priority_csat = Window.partitionBy("priority", "csat")
w_priority_agent_id_by_csat_ti = Window.partitionBy("priority", "agent_id").orderBy(F.col("csat").desc())
w_closed_at_month_agent_id = Window.partitionBy(F.month("closed_at"), "agent_id")
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_by_closed_at_ties = Window.partitionBy("agent_id").orderBy("closed_at")
w_agent_id = Window.partitionBy("agent_id")
w_all_by_closed_at_month_agent = Window.orderBy(F.col("closed_at_month_agent_id_sum_requester_id_filled").desc(), "id").rowsBetween(-6, -1)
w_rn_priority = Window.partitionBy("rn", "priority")
w_all_by_closed_at_month_agent_2 = Window.orderBy(F.col("closed_at_month_agent_id_sum_requester_id_filled").desc(), "id")

scoped = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "refund")) & (F.col("channel") == "web"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.lead("opened_at").over(w_all_by_opened_at).alias("next_opened_at"),
        F.count(F.lit(1)).over(w_priority_csat).alias("rows_in_priority_csat"),
        F.round(F.cume_dist().over(w_priority_agent_id_by_csat_ti), 3).alias("csat_cume_dist"),
        F.sum("requester_id_filled").over(w_closed_at_month_agent_id).alias("closed_at_month_agent_id_sum_requester_id_filled"),
        F.dense_rank().over(w_agent_id_by_closed_at).alias("closed_at_dense_rank"),
        F.round(F.cume_dist().over(w_agent_id_by_closed_at_ties), 3).alias("closed_at_cume_dist"),
        F.row_number().over(w_all_by_opened_at).alias("rn"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_month_agent_id), F.lit(0)), 4).alias("requester_id_share_of_closed_at_month_agent_id"),
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
    )
)

top_per_group = (
    ranked
    .filter(F.col("closed_at_dense_rank") <= 5)
    .drop("closed_at_dense_rank")
)

sequenced = (
    top_per_group
    .select(
        "*",
        F.round(F.avg("requester_id_filled").over(w_all_by_closed_at_month_agent), 2).alias("requester_id_filled_avg_6_prior"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_rn_priority), F.lit(0)), 4).alias("csat_share_of_rn_priority"),
        F.max("rows_in_priority_csat").over(w_rn_priority).alias("rn_priority_max_rows_in_priority_csat"),
        F.size(F.collect_set("agent_id").over(w_rn_priority)).alias("distinct_agent_id_in_rn_priority"),
        F.row_number().over(w_all_by_closed_at_month_agent_2).alias("rn_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "requester_id_filled",
        "next_opened_at",
        "rows_in_priority_csat",
        "csat_cume_dist",
        "closed_at_month_agent_id_sum_requester_id_filled",
        "closed_at_cume_dist",
        "rn",
        "requester_id_share_of_closed_at_month_agent_id",
        "distinct_priority_in_agent_id",
        "requester_id_filled_avg_6_prior",
        "distinct_agent_id_in_rn_priority",
        "rn_2",
        F.abs(F.col("csat") - 4).alias("csat_distance"),
    )
    .orderBy(F.col("rows_in_priority_csat").desc(), F.col("opened_at").desc(), "id")
    .limit(5)
)
