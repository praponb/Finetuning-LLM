from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_opened_at_month_by = Window.partitionBy("agent_id", F.month("opened_at")).orderBy("closed_at", "id")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_all_by_requester_id_full = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id_ties = Window.orderBy(F.col("requester_id").desc())
w_agent_id_by_closed_at_quarte = Window.partitionBy("agent_id").orderBy(F.col("closed_at_quarter").desc(), "id")
w_csat_by_csat_per_requester_i = Window.partitionBy("csat").orderBy(F.col("csat_per_requester_id").desc_nulls_first(), "id")
w_agent_id_by_closed_at_quarte_2 = Window.partitionBy("agent_id").orderBy(F.col("closed_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority = Window.partitionBy("priority")
w_channel_csat_by_seq_no_runni = Window.partitionBy("channel", "csat").orderBy(F.col("seq_no").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat = Window.partitionBy("csat")
w_channel_csat_by_seq_no_ties = Window.partitionBy("channel", "csat").orderBy(F.col("seq_no").desc())
w_channel_csat_by_seq_no_prior = Window.partitionBy("channel", "csat").orderBy(F.col("seq_no").desc(), "id").rowsBetween(-3, -1)

filtered = (
    df_tickets
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
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.lag("csat_per_requester_id").over(w_agent_id_opened_at_month_by).alias("prev_csat_per_requester_id"),
        F.row_number().over(w_all_by_requester_id).alias("seq_no"),
        F.last("opened_at").over(w_all_by_requester_id_full).alias("last_opened_at"),
        F.round(F.cume_dist().over(w_all_by_requester_id_ties), 3).alias("requester_id_cume_dist"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.ntile(5).over(w_agent_id_by_closed_at_quarte).alias("closed_at_quarter_quintile"),
        F.dense_rank().over(w_csat_by_csat_per_requester_i).alias("csat_per_requester_id_dense_rank"),
        F.last("prev_csat_per_requester_id", ignorenulls=True).over(w_agent_id_by_closed_at_quarte_2).alias("last_known_prev_csat_per_requester_id"),
        F.ntile(4).over(w_csat_by_csat_per_requester_i).alias("csat_per_requester_id_quartile"),
        F.size(F.collect_set("channel").over(w_priority)).alias("distinct_channel_in_priority"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "closed_at_quarter",
        "last_opened_at",
        "requester_id_cume_dist",
        "closed_at_quarter_quintile",
        "csat_per_requester_id_dense_rank",
        "last_known_prev_csat_per_requester_id",
        "csat_per_requester_id_quartile",
        F.count(F.lit(1)).over(w_channel_csat_by_seq_no_runni).alias("running_count"),
        F.round(F.col("csat_per_requester_id") - F.avg("csat_per_requester_id").over(w_csat), 2).alias("csat_per_requester_id_vs_csat_avg"),
        F.round(F.cume_dist().over(w_channel_csat_by_seq_no_ties), 3).alias("seq_no_cume_dist"),
        F.sum("csat").over(w_channel_csat_by_seq_no_prior).alias("csat_sum_3_prior"),
        F.round(F.avg("distinct_channel_in_priority").over(w_csat), 2).alias("csat_avg_distinct_channel_in_priority"),
        F.size(F.collect_set("channel").over(w_csat)).alias("distinct_channel_in_csat"),
    )
)

result = (
    sequenced
    .cube("csat", "csat_per_requester_id_dense_rank")
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.min("closed_at_quarter_quintile").alias("min_closed_at_quarter_quintile"),
        F.min("csat_avg_distinct_channel_in_priority").alias("min_csat_avg_distinct_channel_in_priority"),
    )
    .orderBy(
        F.col("csat").asc_nulls_last(),
        F.col("csat_per_requester_id_dense_rank").asc_nulls_last(),
    )
)
