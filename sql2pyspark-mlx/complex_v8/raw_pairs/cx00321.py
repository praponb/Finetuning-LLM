from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_row_count = Window.partitionBy("channel", "row_count")
w_all_by_stddev_days_since_ope = Window.orderBy(F.col("stddev_days_since_opened_at").asc_nulls_last(), "channel")
w_distinct_prioritys_by_distin = Window.partitionBy("distinct_prioritys").orderBy(F.col("distinct_prioritys").desc(), "channel")
w_distinct_prioritys_by_distin_2 = Window.partitionBy("distinct_prioritys").orderBy(F.col("distinct_prioritys").desc())
w_all_by_stddev_days_since_ope_2 = Window.orderBy(F.col("stddev_days_since_opened_at").asc_nulls_last())
w_channel = Window.partitionBy("channel")
w_row_count_seq_no_by_next_std = Window.partitionBy("row_count", "seq_no").orderBy("next_stddev_days_since_opened_at", "channel")
w_all_by_next_stddev_days_sinc = Window.orderBy(F.col("next_stddev_days_since_opened_at").desc())

recent = (
    df_tickets
    .filter((F.to_date(F.col("opened_at")) < F.lit("2025-03-14").cast("date")) & (F.col("closed_at") < F.lit("2025-03-01 12:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 8)
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.days_since_opened_at",
        "su.agent_name",
    )
)

rollup_base = (
    with_details
    .groupBy("channel")
    .agg(
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("agent_name"))).like("K%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_agent_name_like_k"),
        F.round(F.stddev("days_since_opened_at"), 2).alias("stddev_days_since_opened_at"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
)

ranked = (
    rollup_base
    .select(
        "channel",
        "pct_upper_trim_agent_name_like_k",
        "stddev_days_since_opened_at",
        "row_count",
        "agent_name_list",
        "distinct_prioritys",
        F.count(F.lit(1)).over(w_channel_row_count).alias("rows_in_channel_row_count"),
        F.lead("stddev_days_since_opened_at", 1, 0).over(w_all_by_stddev_days_since_ope).alias("next_stddev_days_since_opened_at"),
        F.row_number().over(w_distinct_prioritys_by_distin).alias("seq_no"),
        F.round(F.cume_dist().over(w_distinct_prioritys_by_distin_2), 3).alias("distinct_prioritys_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_stddev_days_since_ope_2), 3).alias("stddev_days_since_opened_at_cume_dist"),
    )
)

sequenced = (
    ranked
    .select(
        "channel",
        "pct_upper_trim_agent_name_like_k",
        "row_count",
        "agent_name_list",
        "distinct_prioritys",
        "rows_in_channel_row_count",
        "next_stddev_days_since_opened_at",
        "distinct_prioritys_cume_dist",
        "stddev_days_since_opened_at_cume_dist",
        F.round(F.avg("seq_no").over(w_channel), 2).alias("channel_avg_seq_no"),
        F.rank().over(w_row_count_seq_no_by_next_std).alias("next_stddev_days_since_opened_at_rank"),
        F.dense_rank().over(w_all_by_next_stddev_days_sinc).alias("next_stddev_days_since_opened_at_dense_rank"),
        F.dense_rank().over(w_row_count_seq_no_by_next_std).alias("next_stddev_days_since_opened_at_dense_rank_2"),
        F.round(F.col("distinct_prioritys") / F.nullif(F.sum("distinct_prioritys").over(w_channel), F.lit(0)), 4).alias("distinct_prioritys_share_of_channel"),
    )
)

result = (
    sequenced
    .cube("next_stddev_days_since_opened_at_dense_rank", "next_stddev_days_since_opened_at_dense_rank_2", "distinct_prioritys")
    .agg(
        F.sum("row_count").alias("total_row_count"),
        F.sum("rows_in_channel_row_count").alias("total_rows_in_channel_row_count"),
    )
    .orderBy(
        F.col("next_stddev_days_since_opened_at_dense_rank").asc_nulls_last(),
        F.col("next_stddev_days_since_opened_at_dense_rank_2").asc_nulls_last(),
        F.col("distinct_prioritys").asc_nulls_last(),
    )
)
