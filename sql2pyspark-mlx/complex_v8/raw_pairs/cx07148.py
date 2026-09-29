from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_agent_tier_by_close = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("closed_at").desc())
w_agent_id_agent_tier_by_close_2 = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("closed_at").desc(), "id")
w_channel_group_closed_at_to_d = Window.partitionBy("channel_group", F.to_date("closed_at")).orderBy("closed_at")
w_channel_group_closed_at_to_d_2 = Window.partitionBy("channel_group", F.to_date("closed_at")).orderBy("closed_at", "id")
w_agent_id_channel_group_by_cl = Window.partitionBy("agent_id", "channel_group").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_group_closed_at_to_d_3 = Window.partitionBy("channel_group", F.to_date("closed_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_agent_id_agent_tier_by_close_3 = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(1, 2)) & (F.col("csat") <= 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        F.when(F.col("channel").isin("web", "email"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.tier").isin("T1", "T3"))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel_group",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.dense_rank().over(w_agent_id_agent_tier_by_close).alias("closed_at_dense_rank"),
        F.lag("closed_at").over(w_agent_id_agent_tier_by_close_2).alias("prev_closed_at"),
        F.round(F.percent_rank().over(w_channel_group_closed_at_to_d), 3).alias("closed_at_percent_rank"),
        F.lead("closed_at", 2).over(w_channel_group_closed_at_to_d_2).alias("next2_closed_at"),
        F.first("closed_at", ignorenulls=True).over(w_agent_id_channel_group_by_cl).alias("first_known_closed_at"),
        F.last("closed_at", ignorenulls=True).over(w_channel_group_closed_at_to_d_3).alias("last_known_closed_at"),
        F.ntile(4).over(w_all_by_closed_at).alias("closed_at_quartile"),
        F.first("closed_at").over(w_agent_id_agent_tier_by_close_3).alias("first_closed_at"),
        F.dense_rank().over(w_channel_group_closed_at_to_d).alias("closed_at_dense_rank_3"),
        F.nth_value("closed_at", 2).over(w_agent_id_agent_tier_by_close_3).alias("second_closed_at"),
        F.row_number().over(w_agent_id_agent_tier_by_close_2).alias("closed_at_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "closed_at",
        "channel_group",
        "closed_at_dense_rank",
        "prev_closed_at",
        "closed_at_percent_rank",
        "first_known_closed_at",
        "closed_at_dense_rank_3",
        "closed_at_seq",
    )
)
