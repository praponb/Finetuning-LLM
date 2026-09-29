from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_tier_by_agent = Window.partitionBy("priority", "agent_tier").orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_requester_id_running = Window.orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_priority_channel_by_agent_hi = Window.partitionBy("priority", "channel").orderBy("agent_hired_on", "id")
w_agent_id_by_requester_id_tie = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last())
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_closed_at_month_by_opened_at = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("opened_at").desc(), "id")
w_agent_tier_closed_at_to_date = Window.partitionBy("agent_tier", F.to_date("closed_at"))
w_priority_channel_by_agent_hi_2 = Window.partitionBy("priority", "channel").orderBy("agent_hired_on", "id").rowsBetween(-1, -1)
w_priority_by_agent_hired_on_m = Window.partitionBy("priority").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(-2, Window.currentRow)

scoped = (
    df_tickets
    .filter((F.col("priority").isNotNull()) & (F.col("agent_id").isNotNull()))
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
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

sequenced = (
    with_parent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "agent_name",
        F.dense_rank().over(w_priority_agent_tier_by_agent).alias("agent_hired_on_dense_rank"),
        F.last("requester_id", ignorenulls=True).over(w_all_by_requester_id_running).alias("last_known_requester_id"),
        F.row_number().over(w_agent_id_by_requester_id).alias("rn"),
        F.dense_rank().over(w_priority_channel_by_agent_hi).alias("agent_hired_on_dense_rank_2"),
        F.round(F.cume_dist().over(w_agent_id_by_requester_id_tie), 3).alias("requester_id_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_requester_id), 3).alias("requester_id_cume_dist_2"),
        F.ntile(10).over(w_closed_at_month_by_opened_at).alias("opened_at_decile"),
        F.min("requester_id").over(w_agent_tier_closed_at_to_date).alias("agent_tier_closed_at_to_date_min_requester_id"),
        F.first("agent_hired_on", ignorenulls=True).over(w_all_by_requester_id_running).alias("first_known_agent_hired_on"),
        F.round(F.avg("requester_id").over(w_priority_channel_by_agent_hi_2), 2).alias("requester_id_avg_1_prior"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_requester_id)).alias("requester_id_change"),
        F.max("requester_id").over(w_priority_by_agent_hired_on_m).alias("requester_id_max_3"),
    )
)

derived = (
    sequenced
    .filter(F.col("priority").isNotNull())
    .select(
        "*",
        F.when(F.col("priority").isin("P4", "P2"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.when(F.col("requester_id_cume_dist") >= 0.8, "large").when(F.col("requester_id_cume_dist") >= 0.2, "medium").otherwise("small").alias("requester_id_cume_dist_tier"),
    )
)

result = (
    derived
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "agent_name",
        "agent_hired_on_dense_rank",
        "last_known_requester_id",
        "rn",
        "requester_id_cume_dist",
        "requester_id_avg_1_prior",
        "requester_id_change",
        "requester_id_max_3",
        "priority_group",
        "requester_id_cume_dist_tier",
    )
)
