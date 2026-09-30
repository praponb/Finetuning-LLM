from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_closed_at_year = Window.partitionBy("channel").orderBy(F.col("closed_at_year").desc(), "id")
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_agent_id_channel_by_opened_a = Window.partitionBy("agent_id", "channel").orderBy("opened_at", "id")
w_closed_at_year_month_channel = Window.partitionBy(F.month("closed_at_year"), "channel").orderBy(F.col("opened_at").desc(), "id")
w_channel_by_closed_at_year_ru = Window.partitionBy("channel").orderBy(F.col("closed_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_by_prev_closed_at_ye = Window.partitionBy("channel").orderBy("prev_closed_at_year", "id")
w_opened_at_seq_last_known_ope = Window.partitionBy("opened_at_seq", F.to_date("last_known_opened_at")).orderBy("opened_at_seq", "id").rowsBetween(-1, -1)
w_opened_at_seq_last_known_ope_2 = Window.partitionBy("opened_at_seq", F.to_date("last_known_opened_at")).orderBy("opened_at_seq")
w_opened_at_seq_opened_at_deci = Window.partitionBy("opened_at_seq", "opened_at_decile")
w_channel_by_prev_closed_at_ye_2 = Window.partitionBy("channel").orderBy("prev_closed_at_year")
w_all_by_total_opened_at_cume = Window.orderBy("total_opened_at_cume_dist", "opened_at_ym", "agent_id", "agent_tier")
w_all_by_total_opened_at_cume_2 = Window.orderBy("total_opened_at_cume_dist", "opened_at_ym", "agent_id", "agent_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter(F.col("csat") >= 2)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
    )
)

enriched_agent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.closed_at_year",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    enriched_agent
    .select(
        "*",
        F.lead("closed_at_year").over(w_channel_by_closed_at_year).alias("next_closed_at_year"),
        F.row_number().over(w_all_by_opened_at).alias("opened_at_seq"),
        F.ntile(10).over(w_all_by_opened_at).alias("opened_at_decile"),
        F.round(F.cume_dist().over(w_agent_id_channel_by_opened_a), 3).alias("opened_at_cume_dist"),
        F.dense_rank().over(w_closed_at_year_month_channel).alias("opened_at_dense_rank"),
        F.last("opened_at", ignorenulls=True).over(w_channel_by_closed_at_year_ru).alias("last_known_opened_at"),
        F.lag("closed_at_year").over(w_agent_id_channel_by_opened_a).alias("prev_closed_at_year"),
        F.round(F.percent_rank().over(w_agent_id_channel_by_opened_a), 3).alias("opened_at_percent_rank"),
        F.round((F.unix_timestamp("closed_at_year") - F.unix_timestamp(F.lag("closed_at_year").over(w_agent_id_channel_by_opened_a))) / 60, 1).alias("minutes_since_prev_closed_at_year"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.rank().over(w_channel_by_prev_closed_at_ye).alias("prev_closed_at_year_rank"),
        F.round(F.avg("opened_at_seq").over(w_opened_at_seq_last_known_ope), 2).alias("opened_at_seq_avg_1_prior"),
        F.ntile(10).over(w_channel_by_prev_closed_at_ye).alias("prev_closed_at_year_decile"),
        F.dense_rank().over(w_opened_at_seq_last_known_ope_2).alias("opened_at_seq_dense_rank"),
        F.max("opened_at_seq").over(w_opened_at_seq_opened_at_deci).alias("opened_at_seq_opened_at_decile_max_opened_at_seq"),
        F.round(F.percent_rank().over(w_channel_by_prev_closed_at_ye_2), 3).alias("prev_closed_at_year_percent_rank"),
        F.dense_rank().over(w_channel_by_prev_closed_at_ye).alias("prev_closed_at_year_dense_rank"),
        F.count(F.lit(1)).over(w_opened_at_seq_opened_at_deci).alias("rows_in_opened_at_seq_opened_at_decile"),
    )
)

summary = (
    windowed
    .groupBy(
        F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"),
        "agent_id",
        "agent_tier",
    )
    .agg(
        F.min("closed_at_year").alias("first_closed_at_year"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.round(F.sum("opened_at_cume_dist"), 2).alias("total_opened_at_cume_dist"),
        F.round(F.stddev("opened_at_percent_rank"), 2).alias("stddev_opened_at_percent_rank"),
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
    )
)

with_trends_2 = (
    summary
    .select(
        "opened_at_ym",
        "agent_id",
        "agent_tier",
        "agent_name_list",
        "total_opened_at_cume_dist",
        "stddev_opened_at_percent_rank",
        "distinct_agent_tiers",
        "channel_list",
        F.round(F.cume_dist().over(w_all_by_total_opened_at_cume), 3).alias("total_opened_at_cume_dist_cume_dist"),
        F.last("distinct_agent_tiers", ignorenulls=True).over(w_all_by_total_opened_at_cume_2).alias("last_known_distinct_agent_tiers"),
    )
)

result = (
    with_trends_2
    .filter(F.size(F.col("agent_name_list")) >= 1)
    .select(
        "opened_at_ym",
        "agent_id",
        "agent_tier",
        "total_opened_at_cume_dist",
        "distinct_agent_tiers",
        "channel_list",
        "total_opened_at_cume_dist_cume_dist",
        "last_known_distinct_agent_tiers",
        F.length(F.trim(F.col("agent_tier"))).alias("agent_tier_len"),
    )
    .orderBy(F.col("distinct_agent_tiers").desc(), "agent_tier")
)
