from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_csat_channel_by_csat_moving = Window.partitionBy("csat", "channel").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_priority_by_csat = Window.partitionBy("priority").orderBy("csat", "id")
w_priority = Window.partitionBy("priority")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_all_by_csat = Window.orderBy(F.col("csat").desc_nulls_first(), "id")
w_all_by_csat_ties = Window.orderBy(F.col("csat").desc_nulls_first())
w_csat_channel_by_csat_full = Window.partitionBy("csat", "channel").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_rank_2_csat_decile = Window.partitionBy("csat_rank_2", "csat_decile")

prepared_tickets = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_details = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.max("csat").over(w_agent_id).alias("agent_id_max_csat"),
        F.round(F.avg("requester_id").over(w_csat_channel_by_csat_moving), 2).alias("requester_id_avg_3"),
        F.ntile(10).over(w_priority_by_csat).alias("csat_decile"),
        F.min("csat").over(w_priority).alias("priority_min_csat"),
        F.round(F.percent_rank().over(w_all_by_requester_id), 3).alias("requester_id_percent_rank"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_priority), F.lit(0)), 4).alias("csat_share_of_priority"),
        F.rank().over(w_all_by_csat).alias("csat_rank"),
        F.rank().over(w_all_by_csat_ties).alias("csat_rank_2"),
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
        F.nth_value("csat", 2).over(w_csat_channel_by_csat_full).alias("second_csat"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "requester_id",
        "agent_id",
        "agent_hired_on",
        "requester_id_avg_3",
        "csat_decile",
        "priority_min_csat",
        "csat_rank",
        "csat_rank_2",
        "distinct_priority_in_agent_id",
        "second_csat",
        F.count(F.lit(1)).over(w_csat_rank_2_csat_decile).alias("rows_in_csat_rank_2_csat_decile"),
        F.min("requester_id_avg_3").over(w_csat_rank_2_csat_decile).alias("csat_rank_2_csat_decile_min_requester_id_avg_3"),
        F.min("requester_id_percent_rank").over(w_csat_rank_2_csat_decile).alias("csat_rank_2_csat_decile_min_requester_id_percent"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "requester_id",
        "agent_id",
        "agent_hired_on",
        "requester_id_avg_3",
        "priority_min_csat",
        "csat_rank",
        "csat_rank_2",
        "distinct_priority_in_agent_id",
        "second_csat",
        "rows_in_csat_rank_2_csat_decile",
        "csat_rank_2_csat_decile_min_requester_id_avg_3",
    )
)
