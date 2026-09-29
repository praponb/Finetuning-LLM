from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_tier = Window.partitionBy("priority", "agent_tier")
w_closed_at_year_agent_id = Window.partitionBy(F.year("closed_at"), "agent_id")
w_csat_priority_by_closed_at = Window.partitionBy("csat", "priority").orderBy(F.col("closed_at").desc(), "id")
w_csat_priority_by_csat_prior = Window.partitionBy("csat", "priority").orderBy("csat", "id").rowsBetween(-3, -1)
w_all_by_closed_at_moving_6 = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_priority_closed_at_to_date_b = Window.partitionBy("priority", F.to_date("closed_at")).orderBy("csat")
w_csat_priority_by_csat_full = Window.partitionBy("csat", "priority").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_priority_by_csat_moving = Window.partitionBy("csat", "priority").orderBy("csat", "id").rowsBetween(-6, Window.currentRow)

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_parent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier") == "T2"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_priority_agent_tier), F.lit(0)), 4).alias("csat_share_of_priority_agent_tier"),
        F.sum("csat").over(w_priority_agent_tier).alias("priority_agent_tier_sum_csat"),
        F.round(F.avg("csat").over(w_closed_at_year_agent_id), 2).alias("closed_at_year_agent_id_avg_csat"),
        F.lead("csat", 1, 0).over(w_csat_priority_by_closed_at).alias("next_csat"),
        F.round(F.avg("requester_id").over(w_csat_priority_by_csat_prior), 2).alias("requester_id_avg_3_prior"),
        F.max("csat").over(w_all_by_closed_at_moving_6).alias("csat_max_7"),
        F.rank().over(w_priority_closed_at_to_date_b).alias("csat_rank"),
        F.nth_value("closed_at", 2).over(w_csat_priority_by_csat_full).alias("second_closed_at"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_closed_at_year_agent_id), F.lit(0)), 4).alias("csat_share_of_closed_at_year_agent_id"),
        F.size(F.collect_set("agent_tier").over(w_closed_at_year_agent_id)).alias("distinct_agent_tier_in_closed_at_year_agent_id"),
        F.max("requester_id").over(w_csat_priority_by_csat_moving).alias("requester_id_max_7"),
    )
)

tagged = (
    metrics
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        "agent_tier",
        "csat_share_of_priority_agent_tier",
        "priority_agent_tier_sum_csat",
        "closed_at_year_agent_id_avg_csat",
        "next_csat",
        "csat_max_7",
        "csat_rank",
        "second_closed_at",
        "csat_share_of_closed_at_year_agent_id",
        "distinct_agent_tier_in_closed_at_year_agent_id",
        "requester_id_max_7",
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

result = (
    tagged
    .select(
        "id",
        "requester_id",
        "priority",
        "csat",
        "agent_tier",
        "priority_agent_tier_sum_csat",
        "closed_at_year_agent_id_avg_csat",
        "next_csat",
        "csat_max_7",
        "csat_rank",
        "second_closed_at",
        "distinct_agent_tier_in_closed_at_year_agent_id",
        "requester_id_max_7",
        "closed_at_hour",
        "priority_prefix",
        "priority_len",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("priority_len").asc_nulls_last())
)
