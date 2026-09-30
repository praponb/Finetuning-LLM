from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc_nulls_first(), "id")
w_channel = Window.partitionBy("channel")
w_all_by_csat_moving_1 = Window.orderBy("csat", "id").rowsBetween(-1, Window.currentRow)
w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_all_by_requester_id_ties = Window.orderBy(F.col("requester_id").desc())
w_all_by_csat_moving_2 = Window.orderBy("csat", "id").rowsBetween(-2, Window.currentRow)
w_all_by_opened_at = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id")

prepared = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-06 15:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

metrics = (
    prepared
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.row_number().over(w_agent_id_csat_by_requester_i).alias("rn"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.round(F.avg("csat").over(w_all_by_csat_moving_1), 2).alias("csat_avg_2"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority_agent_id), 2).alias("requester_id_vs_priority_agent_id_avg"),
        F.round(F.cume_dist().over(w_all_by_requester_id_ties), 3).alias("requester_id_cume_dist"),
        F.size(F.collect_set("priority").over(w_channel)).alias("distinct_priority_in_channel"),
        F.max("csat").over(w_channel).alias("channel_max_csat"),
        F.round(F.avg("csat").over(w_all_by_csat_moving_2), 2).alias("csat_avg_3"),
        F.lead("opened_at", 3).over(w_all_by_opened_at).alias("next3_opened_at"),
        F.round(F.col("csat") - F.avg("csat").over(w_priority_agent_id), 2).alias("csat_vs_priority_agent_id_avg"),
    )
)

scored = (
    metrics
    .filter(F.col("next3_opened_at") < F.lit("2025-02-18 13:00:00").cast("timestamp"))
    .select(
        "*",
        F.round(F.col("csat_avg_3") * 0.9, 2).alias("csat_avg_3_adj"),
        F.round(F.col("csat_avg_2") * 1.07, 2).alias("csat_avg_2_adj"),
    )
)

result = (
    scored
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "rn",
        "csat_avg_2",
        "requester_id_vs_priority_agent_id_avg",
        "requester_id_cume_dist",
        "distinct_priority_in_channel",
        "channel_max_csat",
        "csat_avg_3",
        "next3_opened_at",
        "csat_vs_priority_agent_id_avg",
        "csat_avg_2_adj",
    )
    .orderBy(F.col("csat_avg_2").asc())
)
