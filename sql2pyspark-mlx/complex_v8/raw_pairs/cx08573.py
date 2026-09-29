from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_channel_csat_by_csat_prior_6 = Window.partitionBy("channel", "csat").orderBy(F.col("csat").desc(), "id").rowsBetween(-6, -1)
w_channel_agent_id_by_csat_ful = Window.partitionBy("channel", "agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_csat_by_csat_moving = Window.partitionBy("channel", "csat").orderBy(F.col("csat").desc(), "id").rowsBetween(-1, Window.currentRow)
w_channel_csat_by_csat = Window.partitionBy("channel", "csat").orderBy(F.col("csat").desc(), "id")
w_all_by_csat_avg_6_prior_adj = Window.orderBy(F.col("csat_avg_6_prior_adj").desc(), "id")
w_last_csat_distance = Window.partitionBy("last_csat_distance")
w_all_by_csat_avg_6_prior_adj_2 = Window.orderBy(F.col("csat_avg_6_prior_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(8, 10))
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
    )
)

sequenced = (
    valid
    .select(
        "id",
        "agent_id",
        "csat",
        "tags",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_channel), F.lit(0)), 4).alias("csat_share_of_agent_id_channel"),
        F.round(F.avg("csat").over(w_channel_csat_by_csat_prior_6), 2).alias("csat_avg_6_prior"),
        F.last("csat").over(w_channel_agent_id_by_csat_ful).alias("last_csat"),
        F.sum("csat").over(w_channel_csat_by_csat_moving).alias("csat_sum_2"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_channel), 2).alias("csat_vs_agent_id_channel_avg"),
        F.dense_rank().over(w_channel_csat_by_csat).alias("csat_dense_rank"),
    )
)

enriched = (
    sequenced
    .filter(F.col("csat_dense_rank") <= 9)
    .select(
        "id",
        "csat_share_of_agent_id_channel",
        F.round(F.col("csat_avg_6_prior") * 0.9, 2).alias("csat_avg_6_prior_adj"),
        (F.col("csat_avg_6_prior") - F.col("csat_sum_2")).alias("csat_avg_6_prior_minus_csat_sum_2"),
        F.abs(F.col("last_csat") - 3).alias("last_csat_distance"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w_all_by_csat_avg_6_prior_adj).alias("csat_avg_6_prior_adj_quartile"),
        F.round(F.col("csat_avg_6_prior_adj") / F.nullif(F.sum("csat_avg_6_prior_adj").over(w_last_csat_distance), F.lit(0)), 4).alias("csat_avg_6_prior_adj_share_of_last_csat_distance"),
        F.nth_value("last_csat_distance", 2).over(w_all_by_csat_avg_6_prior_adj_2).alias("second_last_csat_distance"),
        F.count(F.lit(1)).over(w_last_csat_distance).alias("rows_in_last_csat_distance"),
        F.max("csat_avg_6_prior_adj").over(w_last_csat_distance).alias("last_csat_distance_max_csat_avg_6_prior_adj"),
    )
)

result = (
    windowed
    .select(
        "id",
        "csat_avg_6_prior_adj",
        "csat_avg_6_prior_minus_csat_sum_2",
        "last_csat_distance",
        "csat_avg_6_prior_adj_quartile",
        "rows_in_last_csat_distance",
        "last_csat_distance_max_csat_avg_6_prior_adj",
        F.when(F.col("csat_avg_6_prior_adj") >= 5.2, "large").when(F.col("csat_avg_6_prior_adj") >= 3.0, "medium").otherwise("small").alias("csat_avg_6_prior_adj_tier"),
        F.round(F.col("csat_avg_6_prior_adj") * 1.07, 2).alias("csat_avg_6_prior_adj_adj"),
    )
    .orderBy(F.col("id").desc(), F.col("csat_avg_6_prior_minus_csat_sum_2").desc())
)
