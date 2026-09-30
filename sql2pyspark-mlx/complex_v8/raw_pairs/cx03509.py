from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team = Window.partitionBy("agent_team")
w_channel = Window.partitionBy("channel")
w_csat = Window.partitionBy("csat")
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_agent_team_by_csat = Window.partitionBy("agent_id", "agent_team").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_agent_team_by_opened_at = Window.partitionBy("csat", "agent_team").orderBy(F.col("opened_at").asc_nulls_last())
w_opened_at_month_by_opened_at = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_agent_id_by_opened_at_ties = Window.partitionBy("agent_id").orderBy("opened_at")
w_agent_id_by_opened_at_runnin = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat_full = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_by_opened_at_2 = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").asc_nulls_last())
w_distinct_agent_id_in_agent_t = Window.partitionBy("distinct_agent_id_in_agent_team", F.date_trunc("month", "last_known_opened_at")).orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_rank_2_min_opened = Window.partitionBy("opened_at_rank_2", "min_opened_at_dense_rank_2")

filtered = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
    )
)

joined_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        F.col("su.team").alias("agent_team"),
    )
)

sequenced = (
    joined_agent
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_agent_team)).alias("distinct_agent_id_in_agent_team"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel), F.lit(0)), 4).alias("csat_share_of_channel"),
        F.sum("csat").over(w_csat).alias("csat_sum_csat"),
        F.last("opened_at", ignorenulls=True).over(w_all_by_opened_at_running).alias("last_known_opened_at"),
        F.max("csat").over(w_channel).alias("channel_max_csat"),
        F.last("csat").over(w_all_by_opened_at_full).alias("last_csat"),
        F.first("opened_at", ignorenulls=True).over(w_agent_id_agent_team_by_csat).alias("first_known_opened_at"),
        F.rank().over(w_csat_agent_team_by_opened_at).alias("opened_at_rank"),
        F.last("opened_at").over(w_all_by_opened_at_full).alias("last_opened_at"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_opened_at_month_by_opened_at))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.dense_rank().over(w_agent_id_by_opened_at_ties).alias("opened_at_dense_rank"),
        F.rank().over(w_opened_at_month_by_opened_at).alias("opened_at_rank_2"),
        F.first("csat", ignorenulls=True).over(w_agent_id_by_opened_at_runnin).alias("first_known_csat"),
        F.first("csat").over(w_all_by_opened_at_full).alias("first_csat"),
        F.first("csat").over(w_all_by_csat_full).alias("first_csat_2"),
        F.first("opened_at").over(w_opened_at_month_by_opened_at_2).alias("first_opened_at"),
    )
)

annotated = (
    sequenced
    .filter(F.col("opened_at_rank") < 7)
    .select(
        "*",
        F.when(F.col("channel").isin("chat", "phone"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
        (F.col("opened_at") + F.expr("INTERVAL 15 MINUTES")).alias("opened_at_plus_15m"),
    )
)

sequenced_2 = (
    annotated
    .select(
        "id",
        "channel",
        "csat_sum_csat",
        "channel_max_csat",
        "first_known_opened_at",
        "opened_at_rank",
        "last_opened_at",
        "minutes_since_prev_opened_at",
        "opened_at_rank_2",
        "first_known_csat",
        "first_csat",
        "first_opened_at",
        "channel_group",
        F.round(F.percent_rank().over(w_all_by_opened_at_ties), 3).alias("opened_at_percent_rank"),
        F.first("opened_at_plus_15m", ignorenulls=True).over(w_distinct_agent_id_in_agent_t).alias("first_known_opened_at_plus_15m"),
        F.dense_rank().over(w_all_by_opened_at_ties).alias("opened_at_dense_rank_2"),
    )
)

rollup_base = (
    sequenced_2
    .groupBy(
        "opened_at_rank_2",
        F.date_trunc("month", "first_known_opened_at_plus_15m").alias("first_known_opened_at_plus_15m_month"),
        "channel_group",
    )
    .agg(
        F.min("opened_at_dense_rank_2").alias("min_opened_at_dense_rank_2"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

windowed = (
    rollup_base
    .select(
        "*",
        F.round(F.col("min_opened_at_dense_rank_2") / F.nullif(F.sum("min_opened_at_dense_rank_2").over(w_opened_at_rank_2_min_opened), F.lit(0)), 4).alias("min_opened_at_dense_rank_2_share_of_opened_at_ra"),
    )
)

result = (
    windowed
    .select(
        "opened_at_rank_2",
        "first_known_opened_at_plus_15m_month",
        "min_opened_at_dense_rank_2",
        F.initcap(F.trim(F.col("channel_group"))).alias("channel_group_clean"),
    )
    .orderBy(F.col("opened_at_rank_2").desc_nulls_first())
)
