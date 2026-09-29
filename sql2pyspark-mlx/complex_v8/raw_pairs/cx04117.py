from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_closed_at_year_channel = Window.partitionBy(F.year("closed_at"), "channel")
w_channel_agent_id_by_closed_a_2 = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_agent_tier = Window.partitionBy("agent_id", "agent_tier")
w_channel_agent_id_by_closed_a_3 = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_by_opened_at_ties = Window.partitionBy("channel").orderBy(F.col("opened_at").desc())
w_closed_at_year = Window.partitionBy(F.year("closed_at"))
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_all_by_closed_at_running = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_opened_at_rank = Window.partitionBy("agent_tier", "opened_at_rank")
w_agent_tier_by_csat_per_reque = Window.partitionBy("agent_tier").orderBy("csat_per_requester_id_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier_by_csat_per_reque_2 = Window.partitionBy("agent_tier").orderBy("csat_per_requester_id_2", "id")

base_tickets = (
    df_tickets
    .filter((F.col("requester_id") >= 6) & (F.col("priority") == "P2"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id_2"),
    )
)

combined_details_agent = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") != "T2")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

sequenced = (
    combined_details_agent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        "csat_per_requester_id",
        "csat_per_requester_id_2",
        "agent_tier",
        F.lead("closed_at").over(w_channel_agent_id_by_closed_a).alias("next_closed_at"),
        F.round(F.cume_dist().over(w_all_by_closed_at_ties), 3).alias("closed_at_cume_dist"),
        F.max("csat_per_requester_id_2").over(w_closed_at_year_channel).alias("closed_at_year_channel_max_csat_per_requester_id"),
        F.min("csat_per_requester_id").over(w_closed_at_year_channel).alias("closed_at_year_channel_min_csat_per_requester_id"),
        F.min("csat_per_requester_id_2").over(w_channel_agent_id_by_closed_a_2).alias("min_csat_per_requester_id_2_so_far"),
        F.size(F.collect_set("channel").over(w_agent_id_agent_tier)).alias("distinct_channel_in_agent_id_agent_tier"),
        F.first("csat_per_requester_id").over(w_channel_agent_id_by_closed_a_3).alias("first_csat_per_requester_id"),
        F.rank().over(w_channel_by_opened_at_ties).alias("opened_at_rank"),
        F.round(F.col("csat_per_requester_id_2") - F.avg("csat_per_requester_id_2").over(w_closed_at_year_channel), 2).alias("csat_per_requester_id_2_vs_closed_at_year_channe"),
        F.count(F.lit(1)).over(w_closed_at_year).alias("rows_in_closed_at_year"),
        F.dense_rank().over(w_channel_by_opened_at_ties).alias("opened_at_dense_rank"),
    )
)

labeled = (
    sequenced
    .filter(F.col("csat_per_requester_id") > 1.5)
    .select(
        "*",
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
        F.upper(F.trim(F.col("channel"))).alias("channel_clean_2"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.dense_rank().over(w_all_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.ntile(3).over(w_all_by_closed_at).alias("closed_at_tercile"),
        F.first("next_closed_at", ignorenulls=True).over(w_all_by_closed_at_running).alias("first_known_next_closed_at"),
        F.round(F.col("csat_per_requester_id_2") / F.nullif(F.sum("csat_per_requester_id_2").over(w_agent_tier_opened_at_rank), F.lit(0)), 4).alias("csat_per_requester_id_2_share_of_agent_tier_open"),
        F.size(F.collect_set("channel").over(w_agent_tier_opened_at_rank)).alias("distinct_channel_in_agent_tier_opened_at_rank"),
        F.round(F.avg("csat_per_requester_id_2").over(w_agent_tier_opened_at_rank), 2).alias("agent_tier_opened_at_rank_avg_csat_per_requester"),
        F.first("next_closed_at").over(w_agent_tier_by_csat_per_reque).alias("first_next_closed_at"),
        F.round(F.cume_dist().over(w_agent_tier_by_csat_per_reque_2), 3).alias("csat_per_requester_id_2_cume_dist"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.date_trunc("month", "next_closed_at").alias("next_closed_at_month"),
        "channel",
    )
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.max("closed_at").alias("last_closed_at"),
        F.sum("first_csat_per_requester_id").alias("sum_first_csat_per_requester_id_check"),
    )
    .filter(F.col("sum_first_csat_per_requester_id_check") > 3.5)
    .drop("sum_first_csat_per_requester_id_check")
)

result = (
    totals
    .filter(F.to_date(F.col("last_closed_at")) < F.lit("2025-02-26").cast("date"))
    .select(
        "next_closed_at_month",
        "distinct_channels",
        "last_closed_at",
        F.round((F.unix_timestamp(F.col("last_closed_at")) - F.unix_timestamp(F.col("next_closed_at_month"))) / 3600, 1).alias("hours_next_closed_at_month_to_last_closed_at"),
        F.dayofweek(F.col("last_closed_at")).alias("last_closed_at_dayofweek"),
    )
    .orderBy("last_closed_at")
)
