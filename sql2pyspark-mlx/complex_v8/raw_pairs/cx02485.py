from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_priority_or_unknown = Window.partitionBy("priority", "priority_or_unknown").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_channel_by_requeste = Window.partitionBy("priority", "channel").orderBy(F.col("requester_id").desc(), "id")
w_csat_distance_priority_or_un = Window.partitionBy("csat_distance", "priority_or_unknown")
w_agent_id_closed_at_to_date = Window.partitionBy("agent_id", F.to_date("closed_at"))
w_priority_priority_or_unknown_2 = Window.partitionBy("priority", "priority_or_unknown").orderBy(F.col("requester_id").desc(), "id")
w_priority_priority_or_unknown_3 = Window.partitionBy("priority", "priority_or_unknown").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-6, Window.currentRow)
w_priority_or_unknown_by_csat = Window.partitionBy("priority_or_unknown").orderBy(F.col("csat_distance").desc(), "id")
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_channel_by_requeste_2 = Window.partitionBy("priority", "channel").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-2, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")
w_all_by_distinct_channel_in_a = Window.orderBy("distinct_channel_in_agent_id_closed_at_to_date")
w_channel_by_requester_id_chan = Window.partitionBy("channel").orderBy("requester_id_change", "id")
w_all_by_distinct_channel_in_a_2 = Window.orderBy("distinct_channel_in_agent_id_closed_at_to_date", "id")
w_channel_by_requester_id_chan_2 = Window.partitionBy("channel").orderBy("requester_id_change")

scoped = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.array_contains(F.col("tags"), "upgrade")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.abs(F.col("csat") - 4).alias("csat_distance"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat_distance",
        F.sum("csat_distance").over(w_priority_priority_or_unknown).alias("running_csat_distance"),
        F.min("requester_id").over(w_priority_priority_or_unknown).alias("min_requester_id_so_far"),
        (F.col("requester_id") - F.lag("requester_id").over(w_priority_channel_by_requeste)).alias("requester_id_change"),
        F.min("csat_distance").over(w_csat_distance_priority_or_un).alias("csat_distance_priority_or_unknown_min_csat_dista"),
        F.max("csat_distance").over(w_agent_id_closed_at_to_date).alias("agent_id_closed_at_to_date_max_csat_distance"),
        F.dense_rank().over(w_priority_priority_or_unknown_2).alias("requester_id_dense_rank"),
        F.sum("requester_id").over(w_priority_priority_or_unknown_3).alias("requester_id_sum_7"),
        F.size(F.collect_set("channel").over(w_agent_id_closed_at_to_date)).alias("distinct_channel_in_agent_id_closed_at_to_date"),
        F.max("csat_distance").over(w_csat_distance_priority_or_un).alias("csat_distance_priority_or_unknown_max_csat_dista"),
        F.round(F.avg("requester_id").over(w_agent_id_closed_at_to_date), 2).alias("agent_id_closed_at_to_date_avg_requester_id"),
        F.lead("requester_id").over(w_priority_or_unknown_by_csat).alias("next_requester_id"),
        F.nth_value("csat_distance", 2).over(w_all_by_closed_at_full).alias("second_csat_distance"),
        F.round(F.avg("csat_distance").over(w_priority_channel_by_requeste_2), 2).alias("csat_distance_avg_3"),
        F.lead("requester_id").over(w_priority_channel_by_requeste).alias("next_requester_id_2"),
        F.row_number().over(w_priority_channel_by_requeste).alias("requester_id_seq"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "min_requester_id_so_far",
        "csat_distance_priority_or_unknown_min_csat_dista",
        "requester_id_dense_rank",
        "distinct_channel_in_agent_id_closed_at_to_date",
        "csat_distance_priority_or_unknown_max_csat_dista",
        "next_requester_id",
        "second_csat_distance",
        "next_requester_id_2",
        "requester_id_seq",
        F.size(F.collect_set("channel").over(w_agent_id)).alias("distinct_channel_in_agent_id"),
        F.round(F.cume_dist().over(w_all_by_distinct_channel_in_a), 3).alias("distinct_channel_in_agent_id_closed_at_to_date_c"),
        F.dense_rank().over(w_channel_by_requester_id_chan).alias("requester_id_change_dense_rank"),
        F.min("csat_distance_avg_3").over(w_agent_id).alias("agent_id_min_csat_distance_avg_3"),
        (F.col("requester_id_change") - F.lag("requester_id_change").over(w_all_by_distinct_channel_in_a_2)).alias("requester_id_change_change"),
        F.dense_rank().over(w_channel_by_requester_id_chan_2).alias("requester_id_change_dense_rank_2"),
    )
)

summary = (
    sequenced
    .groupBy("requester_id_dense_rank")
    .agg(
        F.sum("requester_id_dense_rank").alias("total_requester_id_dense_rank"),
        F.sum(F.when(F.col("channel") != "web", 1).otherwise(0)).alias("n_channel_web"),
        F.count(F.when(F.col("priority") != "P1", 1)).alias("n_priority_p1"),
        F.round(F.avg("min_requester_id_so_far"), 2).alias("avg_min_requester_id_so_far"),
        F.round(F.stddev("next_requester_id_2"), 2).alias("stddev_next_requester_id_2"),
    )
)

result = (
    summary
    .filter(F.col("total_requester_id_dense_rank") >= 13)
    .select(
        "requester_id_dense_rank",
        "n_priority_p1",
        "avg_min_requester_id_so_far",
        F.when(F.col("requester_id_dense_rank") >= 6, "large").when(F.col("requester_id_dense_rank") >= 3, "medium").otherwise("small").alias("requester_id_dense_rank_tier"),
        F.round(F.col("n_priority_p1") / F.nullif(F.col("n_channel_web"), F.lit(0)), 3).alias("n_priority_p1_per_n_channel_web"),
    )
    .orderBy(
        F.col("n_priority_p1").desc(),
        F.col("avg_min_requester_id_so_far").asc_nulls_last(),
    )
)
