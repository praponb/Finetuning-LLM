from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_priority = Window.partitionBy("priority")
w_csat_agent_id_by_opened_at_r = Window.partitionBy("csat", "agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_by_closed_at = Window.partitionBy("csat").orderBy("closed_at", "id")
w_all_by_closed_at_full = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_csat_agent_id_by_opened_at_m = Window.partitionBy("csat", "agent_id").orderBy("opened_at", "id").rowsBetween(-3, Window.currentRow)
w_priority_by_csat_moving_2 = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id").rowsBetween(-2, Window.currentRow)

recent = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("channel") != "phone"))
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
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
        F.dense_rank().over(w_all_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
        F.sum("csat").over(w_csat_agent_id_by_opened_at_r).alias("running_csat"),
        F.lead("requester_id").over(w_csat_by_closed_at).alias("next_requester_id"),
        F.nth_value("opened_at", 2).over(w_all_by_closed_at_full).alias("second_opened_at"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.lead("opened_at").over(w_all_by_closed_at).alias("next_opened_at"),
        F.max("csat").over(w_csat_agent_id_by_opened_at_m).alias("csat_max_4"),
        F.max("csat").over(w_priority_by_csat_moving_2).alias("csat_max_3"),
        F.min("requester_id").over(w_agent_id).alias("agent_id_min_requester_id"),
    )
)

result = (
    with_trends
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
        "distinct_priority_in_agent_id",
        "closed_at_dense_rank",
        "rows_in_priority",
        "running_csat",
        "second_opened_at",
        "next_opened_at",
        "agent_id_min_requester_id",
    )
)
