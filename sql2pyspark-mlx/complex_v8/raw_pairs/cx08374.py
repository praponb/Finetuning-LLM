from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_channel = Window.partitionBy("csat", "channel")
w_agent_id = Window.partitionBy("agent_id")
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy("csat", "id")
w_csat_priority_by_csat = Window.partitionBy("csat", "priority").orderBy(F.col("csat").desc(), "id")
w_agent_id_by_csat_ties = Window.partitionBy("agent_id").orderBy(F.col("csat").desc())
w_csat_by_csat_full = Window.partitionBy("csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_csat_ties = Window.partitionBy("csat").orderBy("csat")
w_agent_id_by_csat_running = Window.partitionBy("agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat_percent_rank_pri = Window.orderBy(F.col("csat_percent_rank").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_all_by_csat_percent_rank_pri_2 = Window.orderBy(F.col("csat_percent_rank").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_all_by_second_csat = Window.orderBy("second_csat", "id")
w_agent_id_csat_rank_by_last_k = Window.partitionBy("agent_id", "csat_rank").orderBy(F.col("last_known_csat").desc(), "id")
w_all_by_second_csat_running = Window.orderBy("second_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_rank_3 = Window.partitionBy("csat_rank_3")

prepared = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
    )
)

ranked = (
    prepared
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_channel), F.lit(0)), 4).alias("csat_share_of_csat_channel"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id), F.lit(0)), 4).alias("csat_share_of_agent_id"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_channel), 2).alias("csat_vs_csat_channel_avg"),
        F.min("csat").over(w_agent_id).alias("agent_id_min_csat"),
        F.rank().over(w_agent_id_by_csat).alias("csat_rank"),
        F.round(F.percent_rank().over(w_csat_priority_by_csat), 3).alias("csat_percent_rank"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.round(F.percent_rank().over(w_agent_id_by_csat_ties), 3).alias("csat_percent_rank_2"),
        F.nth_value("csat", 2).over(w_csat_by_csat_full).alias("second_csat"),
        F.rank().over(w_csat_priority_by_csat).alias("csat_rank_2"),
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id), 2).alias("csat_vs_agent_id_avg"),
        F.round(F.percent_rank().over(w_csat_by_csat_ties), 3).alias("csat_percent_rank_3"),
        F.last("csat", ignorenulls=True).over(w_agent_id_by_csat_running).alias("last_known_csat"),
        F.rank().over(w_agent_id_by_csat_ties).alias("csat_rank_3"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.round(F.avg("csat").over(w_all_by_csat_percent_rank_pri), 2).alias("csat_avg_2_prior"),
        F.max("csat_share_of_csat_channel").over(w_all_by_csat_percent_rank_pri_2).alias("csat_share_of_csat_channel_max_6_prior"),
        F.round(F.percent_rank().over(w_all_by_second_csat), 3).alias("second_csat_percent_rank"),
        (F.col("last_known_csat") - F.lag("last_known_csat").over(w_all_by_second_csat)).alias("last_known_csat_change"),
        F.row_number().over(w_agent_id_csat_rank_by_last_k).alias("seq_no"),
        F.lag("csat_rank", 3, 0).over(w_all_by_second_csat).alias("prev3_csat_rank"),
        F.first("csat_vs_agent_id_avg", ignorenulls=True).over(w_all_by_second_csat_running).alias("first_known_csat_vs_agent_id_avg"),
        F.round(F.col("csat_percent_rank") / F.nullif(F.sum("csat_percent_rank").over(w_csat_rank_3), F.lit(0)), 4).alias("csat_percent_rank_share_of_csat_rank_3"),
    )
)

stats = (
    metrics
    .groupBy("last_known_csat_change")
    .agg(
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.max("csat_avg_2_prior").alias("max_csat_avg_2_prior"),
        F.count("*").alias("cnt"),
        F.sum(F.when(~F.col("priority").isin("P2", "P1"), 1).otherwise(0)).alias("n_priority_not_in_p2_p1"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
    )
)

result = (
    stats
    .select(
        "last_known_csat_change",
        "cnt",
        F.round(F.col("avg_csat") * 1.5, 2).alias("avg_csat_adj"),
    )
    .orderBy(F.col("avg_csat_adj").desc())
)
