from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_opened_at_full = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id")
w_priority_by_opened_at_ties = Window.partitionBy("priority").orderBy(F.col("opened_at").desc())
w_channel_agent_tier_by_opened = Window.partitionBy("channel", "agent_tier").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_channel_agent_tier_by_opened_2 = Window.partitionBy("channel", "agent_tier").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_opened_at_runnin = Window.partitionBy("priority").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_percent_ran = Window.orderBy("opened_at_percent_rank_plus_opened_at_cume_dist_", "id")
w_all_by_opened_at_percent_ran_2 = Window.orderBy("opened_at_percent_rank_plus_opened_at_cume_dist_")
w_priority_channel_by_next_ope = Window.partitionBy("priority", "channel").orderBy(F.col("next_opened_at").asc_nulls_last(), "id")
w_csat_tier = Window.partitionBy("csat_tier")
w_priority_channel_by_next_ope_2 = Window.partitionBy("priority", "channel").orderBy(F.col("next_opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_channel_by_next_ope_3 = Window.partitionBy("priority", "channel").orderBy(F.col("next_opened_at").asc_nulls_last())

filtered = (
    df_tickets
    .filter(F.col("requester_id") <= 21)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
    )
)

enriched_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.csat_tier",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    enriched_agent
    .select(
        "*",
        F.nth_value("opened_at", 2).over(w_agent_id_by_opened_at_full).alias("second_opened_at"),
        F.round(F.percent_rank().over(w_agent_id_by_opened_at), 3).alias("opened_at_percent_rank"),
        F.lead("opened_at").over(w_agent_id_by_opened_at).alias("next_opened_at"),
        F.round(F.cume_dist().over(w_priority_by_opened_at_ties), 3).alias("opened_at_cume_dist"),
        F.lag("opened_at").over(w_channel_agent_tier_by_opened).alias("prev_opened_at"),
        F.nth_value("opened_at", 2).over(w_channel_agent_tier_by_opened_2).alias("second_opened_at_2"),
        F.last("opened_at", ignorenulls=True).over(w_priority_by_opened_at_runnin).alias("last_known_opened_at"),
        F.round(F.cume_dist().over(w_channel_agent_tier_by_opened), 3).alias("opened_at_cume_dist_2"),
    )
)

annotated = (
    with_trends
    .filter(F.col("opened_at") >= F.lit("2025-01-13 07:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce(F.col("opened_at_percent_rank"), F.lit(0.0)).alias("opened_at_percent_rank_filled"),
        (F.col("opened_at_percent_rank") + F.col("opened_at_cume_dist_2")).alias("opened_at_percent_rank_plus_opened_at_cume_dist_"),
        F.when(F.col("opened_at_cume_dist") >= 0.6, "HIGH").when(F.col("opened_at_cume_dist") >= 0.5, "MEDIUM").otherwise("LOW").alias("opened_at_cume_dist_tier"),
    )
)

with_trends_2 = (
    annotated
    .select(
        "id",
        "opened_at",
        "priority",
        "tags",
        "csat_tier",
        "agent_tier",
        "second_opened_at",
        "opened_at_cume_dist",
        "prev_opened_at",
        "opened_at_cume_dist_2",
        "opened_at_percent_rank_filled",
        "opened_at_cume_dist_tier",
        F.lag("opened_at_percent_rank", 2, 0).over(w_all_by_opened_at_percent_ran).alias("prev2_opened_at_percent_rank"),
        F.dense_rank().over(w_all_by_opened_at_percent_ran_2).alias("opened_at_percent_rank_plus_opened_at_cume_dist__2"),
        F.lead("opened_at").over(w_priority_channel_by_next_ope).alias("next_opened_at_2"),
        F.sum("opened_at_cume_dist_2").over(w_csat_tier).alias("csat_tier_sum_opened_at_cume_dist_2"),
        F.last("opened_at_cume_dist_2", ignorenulls=True).over(w_priority_channel_by_next_ope_2).alias("last_known_opened_at_cume_dist_2"),
        F.round(F.col("opened_at_cume_dist") / F.nullif(F.sum("opened_at_cume_dist").over(w_csat_tier), F.lit(0)), 4).alias("opened_at_cume_dist_share_of_csat_tier"),
        F.dense_rank().over(w_priority_channel_by_next_ope_3).alias("next_opened_at_dense_rank"),
        F.max("opened_at_percent_rank").over(w_priority_channel_by_next_ope_2).alias("max_opened_at_percent_rank_so_far"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "opened_at",
        "priority",
        "tags",
        "csat_tier",
        "agent_tier",
        "opened_at_cume_dist",
        "opened_at_cume_dist_2",
        "opened_at_cume_dist_tier",
        "prev2_opened_at_percent_rank",
        "opened_at_percent_rank_plus_opened_at_cume_dist__2",
        "next_opened_at_2",
        "csat_tier_sum_opened_at_cume_dist_2",
        "last_known_opened_at_cume_dist_2",
        "opened_at_cume_dist_share_of_csat_tier",
        "max_opened_at_percent_rank_so_far",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
    .orderBy(F.col("last_known_opened_at_cume_dist_2").desc())
)
