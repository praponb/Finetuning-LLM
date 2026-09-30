from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-6, Window.currentRow)
w_agent_tier_csat_by_requester = Window.partitionBy("agent_tier", "csat").orderBy("requester_id", "id").rowsBetween(-6, Window.currentRow)
w_agent_tier = Window.partitionBy("agent_tier")
w_agent_id = Window.partitionBy("agent_id")
w_agent_tier_csat_by_requester_2 = Window.partitionBy("agent_tier", "csat").orderBy("requester_id", "id")
w_channel_agent_id_by_csat_ful = Window.partitionBy("channel", "agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_agent_tier_by_csat_ties = Window.partitionBy("csat", "agent_tier").orderBy("csat")
w_agent_id_csat_by_requester_i_2 = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc(), "id")
w_agent_id_csat_by_requester_i_3 = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_agent_tier_by_csat_full = Window.partitionBy("csat", "agent_tier").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_distinct_agent_id_in_agent_t = Window.partitionBy("distinct_agent_id_in_agent_tier").orderBy("agent_tier_min_requester_id", "id")
w_distinct_agent_id_in_agent_t_2 = Window.partitionBy("distinct_agent_id_in_agent_tier", "agent_id").orderBy("minutes_since_prev_closed_at")
w_agent_tier_first_known_csat = Window.partitionBy("agent_tier", "first_known_csat")
w_channel_csat_dense_rank_by_r = Window.partitionBy("channel", "csat_dense_rank").orderBy("requester_id_share_of_agent_id", "id")

filtered_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 2) & (F.col("channel") == "phone"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

joined_agent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") == "T2")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    joined_agent
    .select(
        "*",
        F.max("requester_id").over(w_agent_id_csat_by_requester_i).alias("requester_id_max_7"),
        F.round(F.avg("requester_id").over(w_agent_tier_csat_by_requester), 2).alias("requester_id_avg_7"),
        F.size(F.collect_set("agent_id").over(w_agent_tier)).alias("distinct_agent_id_in_agent_tier"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_id"),
        F.max("requester_id").over(w_agent_id).alias("agent_id_max_requester_id"),
        F.ntile(3).over(w_agent_tier_csat_by_requester_2).alias("requester_id_tercile"),
        F.size(F.collect_set("agent_tier").over(w_agent_id)).alias("distinct_agent_tier_in_agent_id"),
        F.last("requester_id").over(w_channel_agent_id_by_csat_ful).alias("last_requester_id"),
        F.min("csat").over(w_agent_tier).alias("agent_tier_min_csat"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.dense_rank().over(w_csat_agent_tier_by_csat_ties).alias("csat_dense_rank"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_agent_id_csat_by_requester_i_2))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.first("csat", ignorenulls=True).over(w_agent_id_csat_by_requester_i_3).alias("first_known_csat"),
        F.min("requester_id").over(w_agent_tier).alias("agent_tier_min_requester_id"),
        F.last("requester_id").over(w_csat_agent_tier_by_csat_full).alias("last_requester_id_2"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "agent_tier",
        "requester_id_avg_7",
        "distinct_agent_id_in_agent_tier",
        "requester_id_share_of_agent_id",
        "agent_id_max_requester_id",
        "distinct_agent_tier_in_agent_id",
        "rows_in_agent_id",
        "csat_dense_rank",
        "minutes_since_prev_closed_at",
        "first_known_csat",
        "agent_tier_min_requester_id",
        F.lag("agent_id_max_requester_id").over(w_all_by_closed_at).alias("prev_agent_id_max_requester_id"),
        F.ntile(10).over(w_distinct_agent_id_in_agent_t).alias("agent_tier_min_requester_id_decile"),
        F.round(F.percent_rank().over(w_distinct_agent_id_in_agent_t_2), 3).alias("minutes_since_prev_closed_at_percent_rank"),
        F.round(F.col("csat_dense_rank") / F.nullif(F.sum("csat_dense_rank").over(w_agent_tier_first_known_csat), F.lit(0)), 4).alias("csat_dense_rank_share_of_agent_tier_first_known_"),
        F.round(F.percent_rank().over(w_distinct_agent_id_in_agent_t), 3).alias("agent_tier_min_requester_id_percent_rank"),
        (F.col("minutes_since_prev_closed_at") - F.lag("minutes_since_prev_closed_at").over(w_channel_csat_dense_rank_by_r)).alias("minutes_since_prev_closed_at_change"),
        F.row_number().over(w_all_by_closed_at).alias("rn"),
        F.max("last_requester_id_2").over(w_agent_tier_first_known_csat).alias("agent_tier_first_known_csat_max_last_requester_i"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "agent_id",
        "requester_id_avg_7",
        "distinct_agent_id_in_agent_tier",
        "requester_id_share_of_agent_id",
        "agent_id_max_requester_id",
        "distinct_agent_tier_in_agent_id",
        "rows_in_agent_id",
        "csat_dense_rank",
        "minutes_since_prev_closed_at",
        "first_known_csat",
        "agent_tier_min_requester_id",
        "prev_agent_id_max_requester_id",
        "agent_tier_min_requester_id_decile",
        "minutes_since_prev_closed_at_percent_rank",
        "csat_dense_rank_share_of_agent_tier_first_known_",
        "agent_tier_min_requester_id_percent_rank",
        "minutes_since_prev_closed_at_change",
        "rn",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat_dense_rank").cast("string")).alias("channel_ref"),
        F.concat(F.col("agent_tier"), F.lit("#"), F.col("csat").cast("string")).alias("agent_tier_ref"),
    )
)
