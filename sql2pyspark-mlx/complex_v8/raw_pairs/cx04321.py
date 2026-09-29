from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat = Window.orderBy("csat", "id")
w_opened_at_dayofweek = Window.partitionBy("opened_at_dayofweek")
w_csat_by_closed_at_running = Window.partitionBy("csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_by_closed_at_full = Window.partitionBy("csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_closed_at_ties = Window.partitionBy("csat").orderBy(F.col("closed_at").desc())
w_channel_agent_id_by_opened_a = Window.partitionBy("channel", "agent_id").orderBy("opened_at")
w_all_by_csat_ties = Window.orderBy("csat")
w_channel_agent_id_by_opened_a_2 = Window.partitionBy("channel", "agent_id").orderBy("opened_at", "id")
w_channel = Window.partitionBy("channel")
w_priority_opened_at_dense_ran = Window.partitionBy("priority", "opened_at_dense_rank").orderBy("opened_at_dayofweek_max_channel_len", "id").rowsBetween(-1, -1)
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_year_by_closed_at = Window.partitionBy(F.year("opened_at")).orderBy("closed_at_percent_rank", "id")
w_all_by_first_opened_at_full = Window.orderBy("first_opened_at", "last_channel_len", "opened_at_ym", "opened_at_dayofweek").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_opened_at = Window.orderBy("first_opened_at", "last_channel_len", "opened_at_ym", "opened_at_dayofweek")

scoped_tickets = (
    df_tickets
    .filter((F.col("requester_id") < 23) & (F.col("agent_id") <= 3))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
    )
)

ranked = (
    scoped_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "opened_at_dayofweek",
        F.ntile(4).over(w_all_by_csat).alias("csat_quartile"),
        F.size(F.collect_set("agent_id").over(w_opened_at_dayofweek)).alias("distinct_agent_id_in_opened_at_dayofweek"),
        F.min("csat").over(w_csat_by_closed_at_running).alias("min_csat_so_far"),
        F.max("channel_len").over(w_opened_at_dayofweek).alias("opened_at_dayofweek_max_channel_len"),
        F.lead("csat").over(w_all_by_csat).alias("next_csat"),
        F.last("channel_len").over(w_csat_by_closed_at_full).alias("last_channel_len"),
        F.sum("opened_at_dayofweek").over(w_opened_at_dayofweek).alias("opened_at_dayofweek_sum_opened_at_dayofweek"),
        F.round(F.percent_rank().over(w_csat_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.dense_rank().over(w_channel_agent_id_by_opened_a).alias("opened_at_dense_rank"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_opened_at_dayofweek), F.lit(0)), 4).alias("csat_share_of_opened_at_dayofweek"),
        F.round(F.cume_dist().over(w_all_by_csat_ties), 3).alias("csat_cume_dist"),
        F.lead("closed_at").over(w_channel_agent_id_by_opened_a_2).alias("next_closed_at"),
        F.count(F.lit(1)).over(w_opened_at_dayofweek).alias("rows_in_opened_at_dayofweek"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "opened_at_dayofweek",
        "csat_quartile",
        "distinct_agent_id_in_opened_at_dayofweek",
        "min_csat_so_far",
        "next_csat",
        "last_channel_len",
        "opened_at_dayofweek_sum_opened_at_dayofweek",
        "opened_at_dense_rank",
        "csat_cume_dist",
        F.round(F.col("min_csat_so_far") - F.avg("min_csat_so_far").over(w_channel), 2).alias("min_csat_so_far_vs_channel_avg"),
        F.round(F.avg("min_csat_so_far").over(w_priority_opened_at_dense_ran), 2).alias("min_csat_so_far_avg_1_prior"),
        F.last("csat_quartile").over(w_all_by_closed_at_full).alias("last_csat_quartile"),
        F.dense_rank().over(w_opened_at_year_by_closed_at).alias("closed_at_percent_rank_dense_rank"),
    )
)

rollup_base = (
    with_windows
    .groupBy(
        "last_channel_len",
        F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"),
        "opened_at_dayofweek",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.min("opened_at").alias("first_opened_at"),
        F.min("opened_at_dayofweek").alias("min_opened_at_dayofweek"),
        F.max("closed_at").alias("last_closed_at"),
    )
)

windowed = (
    rollup_base
    .withColumn("second_distinct_agent_ids", F.nth_value("distinct_agent_ids", 2).over(w_all_by_first_opened_at_full))
    .withColumn("next3_last_closed_at", F.lead("last_closed_at", 3).over(w_all_by_first_opened_at))
)

result = (
    windowed
    .select(
        "last_channel_len",
        "opened_at_ym",
        "opened_at_dayofweek",
        "distinct_agent_ids",
        "first_opened_at",
        "min_opened_at_dayofweek",
        "last_closed_at",
        "second_distinct_agent_ids",
        F.when(F.col("last_channel_len") >= 10, "large").when(F.col("last_channel_len") >= 5, "medium").otherwise("small").alias("last_channel_len_tier"),
    )
    .orderBy(
        "distinct_agent_ids",
        "last_channel_len",
        "opened_at_ym",
        "opened_at_dayofweek",
    )
    .limit(10)
)
