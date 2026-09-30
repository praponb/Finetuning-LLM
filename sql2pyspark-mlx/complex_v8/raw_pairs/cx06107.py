from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_requester_id = Window.partitionBy("channel").orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_channel_by_requester_id_full = Window.partitionBy("channel").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_group_by_closed_at = Window.partitionBy("channel_group").orderBy("closed_at", "id")
w_channel = Window.partitionBy("channel")
w_channel_group_by_closed_at_p = Window.partitionBy("channel_group").orderBy("closed_at", "id").rowsBetween(-1, -1)
w_closed_at_month = Window.partitionBy(F.month("closed_at"))
w_channel_by_requester_id_2 = Window.partitionBy("channel").orderBy("requester_id", "id")
w_channel_by_requester_id_ties = Window.partitionBy("channel").orderBy(F.col("requester_id").asc_nulls_last())
w_agent_tier_by_requester_id_b = Window.partitionBy("agent_tier").orderBy("requester_id_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_group = Window.partitionBy("channel_group")
w_agent_team_requester_id_quin = Window.partitionBy("agent_team", "requester_id_quintile")
w_channel_group_agent_id_by_re = Window.partitionBy("channel_group", "agent_id").orderBy(F.col("requester_id_sum_1_prior").desc_nulls_first(), "id")
w_channel_group_agent_id_by_re_2 = Window.partitionBy("channel_group", "agent_id").orderBy(F.col("requester_id_sum_1_prior").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_group_agent_id_by_re_3 = Window.partitionBy("channel_group", "agent_id").orderBy(F.col("requester_id_sum_1_prior").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id_bucket_v = Window.orderBy("requester_id_bucket_vs_closed_at_month_avg", "id")
w_all_by_avg_channel_avg_reque = Window.orderBy("avg_channel_avg_requester_id", "agent_id")
w_distinct_channel_groups_agen = Window.partitionBy("distinct_channel_groups", "agent_id")

base_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "refund"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.when(F.col("channel").isin("phone", "web"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
    )
)

with_details_agent = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

windowed = (
    with_details_agent
    .select(
        "*",
        F.ntile(5).over(w_channel_by_requester_id).alias("requester_id_quintile"),
        F.first("requester_id_bucket").over(w_channel_by_requester_id_full).alias("first_requester_id_bucket"),
        F.lead("requester_id").over(w_channel_group_by_closed_at).alias("next_requester_id"),
        F.round(F.avg("requester_id").over(w_channel), 2).alias("channel_avg_requester_id"),
        F.sum("requester_id").over(w_channel_group_by_closed_at_p).alias("requester_id_sum_1_prior"),
        F.min("requester_id").over(w_closed_at_month).alias("closed_at_month_min_requester_id"),
        F.lead("requester_id_bucket", 3, 0).over(w_channel_by_requester_id_2).alias("next3_requester_id_bucket"),
        F.round(F.col("requester_id_bucket") - F.avg("requester_id_bucket").over(w_closed_at_month), 2).alias("requester_id_bucket_vs_closed_at_month_avg"),
        F.round(F.percent_rank().over(w_channel_by_requester_id_ties), 3).alias("requester_id_percent_rank"),
        F.first("requester_id_bucket", ignorenulls=True).over(w_agent_tier_by_requester_id_b).alias("first_known_requester_id_bucket"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel_group), F.lit(0)), 4).alias("requester_id_share_of_channel_group"),
    )
)

annotated = (
    windowed
    .filter(F.col("channel_group").isin("other", "group_b"))
    .select(
        "*",
        F.coalesce(F.col("requester_id_bucket"), F.lit(0)).alias("requester_id_bucket_filled"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.sum("requester_id_sum_1_prior").over(w_agent_team_requester_id_quin).alias("agent_team_requester_id_quintile_sum_requester_i"),
        F.round(F.col("requester_id_bucket_filled") - F.avg("requester_id_bucket_filled").over(w_agent_team_requester_id_quin), 2).alias("requester_id_bucket_filled_vs_agent_team_request"),
        F.size(F.collect_set("priority").over(w_agent_team_requester_id_quin)).alias("distinct_priority_in_agent_team_requester_id_qui"),
        (F.col("next3_requester_id_bucket") - F.lag("next3_requester_id_bucket").over(w_channel_group_agent_id_by_re)).alias("next3_requester_id_bucket_change"),
        F.min("first_requester_id_bucket").over(w_channel_group_agent_id_by_re_2).alias("min_first_requester_id_bucket_so_far"),
        F.nth_value("requester_id_quintile", 2).over(w_channel_group_agent_id_by_re_3).alias("second_requester_id_quintile"),
        F.lag("closed_at").over(w_all_by_requester_id_bucket_v).alias("prev_closed_at"),
    )
)

summary = (
    metrics
    .groupBy("agent_id")
    .agg(
        F.countDistinct("channel_group").alias("distinct_channel_groups"),
        F.round(F.avg("channel_avg_requester_id"), 2).alias("avg_channel_avg_requester_id"),
        F.percentile_approx("next3_requester_id_bucket", 0.5).alias("median_next3_requester_id_bucket"),
        F.count(F.when(F.col("agent_tier") == "T3", 1)).alias("n_agent_tier_t3"),
        F.max("requester_id_share_of_channel_group").alias("max_requester_id_share_of_channel_group_check"),
    )
    .filter(F.col("max_requester_id_share_of_channel_group_check") > 0.4)
    .drop("max_requester_id_share_of_channel_group_check")
)

sequenced = (
    summary
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_avg_channel_avg_reque), 3).alias("avg_channel_avg_requester_id_cume_dist"),
        F.row_number().over(w_all_by_avg_channel_avg_reque).alias("avg_channel_avg_requester_id_seq"),
        F.max("avg_channel_avg_requester_id").over(w_distinct_channel_groups_agen).alias("distinct_channel_groups_agent_id_max_avg_channel"),
        F.count(F.lit(1)).over(w_distinct_channel_groups_agen).alias("rows_in_distinct_channel_groups_agent_id"),
    )
)

result = (
    sequenced
    .filter(F.col("n_agent_tier_t3") >= 4)
    .select(
        "agent_id",
        "distinct_channel_groups",
        "avg_channel_avg_requester_id",
        "avg_channel_avg_requester_id_cume_dist",
        "avg_channel_avg_requester_id_seq",
        "distinct_channel_groups_agent_id_max_avg_channel",
        "rows_in_distinct_channel_groups_agent_id",
    )
    .orderBy(
        F.col("distinct_channel_groups").desc(),
        F.col("rows_in_distinct_channel_groups_agent_id").desc_nulls_first(),
    )
)
