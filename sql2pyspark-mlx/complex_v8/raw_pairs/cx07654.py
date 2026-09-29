from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_closed_at_prior_3 = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(-3, -1)
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_csat = Window.partitionBy("csat")
w_channel_by_closed_at = Window.partitionBy("channel").orderBy(F.col("closed_at").desc(), "id")
w_agent_id = Window.partitionBy("agent_id")
w_all_by_csat_ties = Window.orderBy("csat")
w_channel_by_closed_at_ties = Window.partitionBy("channel").orderBy(F.col("closed_at").desc())
w_channel = Window.partitionBy("channel")
w_priority_csat_by_prev2_opene = Window.partitionBy("priority", "csat").orderBy("prev2_opened_at", "id")

scoped = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.round(F.avg("csat").over(w_channel_by_closed_at_prior_3), 2).alias("csat_avg_3_prior"),
        F.lag("closed_at").over(w_all_by_closed_at).alias("prev_closed_at"),
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
        F.lag("opened_at", 2).over(w_channel_by_closed_at).alias("prev2_opened_at"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.rank().over(w_all_by_csat_ties).alias("csat_rank"),
        F.sum("csat").over(w_csat).alias("csat_sum_csat"),
        F.round(F.cume_dist().over(w_channel_by_closed_at_ties), 3).alias("closed_at_cume_dist"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "csat_avg_3_prior",
        "distinct_agent_id_in_csat",
        "prev2_opened_at",
        "rows_in_agent_id",
        "csat_rank",
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.size(F.collect_set("priority").over(w_channel)).alias("distinct_priority_in_channel"),
        F.lag("prev2_opened_at", 3).over(w_priority_csat_by_prev2_opene).alias("prev3_prev2_opened_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "closed_at",
        "priority",
        "tags",
        "csat_avg_3_prior",
        "distinct_agent_id_in_csat",
        "prev2_opened_at",
        "csat_rank",
        "distinct_agent_id_in_channel",
        "prev3_prev2_opened_at",
    )
)
