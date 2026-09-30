from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_all_by_csat_full = Window.orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_moving_2 = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_priority_csat = Window.partitionBy("priority", "csat")
w_all_by_opened_at_prior_1 = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_priority_agent_tier = Window.partitionBy("priority", "agent_tier")
w_csat_distinct_agent_tier_in = Window.partitionBy("csat", "distinct_agent_tier_in_priority_csat").orderBy("distinct_agent_tier_in_priority_csat", "id")
w_distinct_agent_tier_in_prior = Window.partitionBy("distinct_agent_tier_in_priority_csat")
w_all_by_priority_csat_max_csa = Window.orderBy(F.col("priority_csat_max_csat").desc_nulls_first(), "id")
w_csat_distinct_agent_tier_in_2 = Window.partitionBy("csat", "distinct_agent_tier_in_priority_csat").orderBy("distinct_agent_tier_in_priority_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("closed_at") < F.lit("2025-02-26 16:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
)

joined_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.priority_prefix",
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    joined_agent
    .select(
        "*",
        F.lead("csat").over(w_all_by_opened_at).alias("next_csat"),
        F.first("csat").over(w_all_by_csat_full).alias("first_csat"),
        F.last("opened_at", ignorenulls=True).over(w_all_by_opened_at_running).alias("last_known_opened_at"),
        F.sum("csat").over(w_all_by_opened_at_moving_2).alias("csat_sum_3"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_priority_csat), F.lit(0)), 4).alias("csat_share_of_priority_csat"),
        F.max("csat").over(w_all_by_opened_at_prior_1).alias("csat_max_1_prior"),
        F.first("opened_at").over(w_all_by_csat_full).alias("first_opened_at"),
        F.max("csat").over(w_priority_csat).alias("priority_csat_max_csat"),
        F.size(F.collect_set("agent_tier").over(w_priority_csat)).alias("distinct_agent_tier_in_priority_csat"),
        F.sum("csat").over(w_priority_csat).alias("priority_csat_sum_csat"),
        F.last("opened_at").over(w_all_by_csat_full).alias("last_opened_at"),
        F.sum("csat").over(w_priority_agent_tier).alias("priority_agent_tier_sum_csat"),
        F.size(F.collect_set("agent_id").over(w_priority_agent_tier)).alias("distinct_agent_id_in_priority_agent_tier"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.lead("csat", 1, 0).over(w_csat_distinct_agent_tier_in).alias("next_csat_2"),
        F.lead("csat_share_of_priority_csat").over(w_csat_distinct_agent_tier_in).alias("next_csat_share_of_priority_csat"),
        F.size(F.collect_set("agent_id").over(w_distinct_agent_tier_in_prior)).alias("distinct_agent_id_in_distinct_agent_tier_in_prio"),
        F.round(F.percent_rank().over(w_all_by_priority_csat_max_csa), 3).alias("priority_csat_max_csat_percent_rank"),
        F.first("next_csat", ignorenulls=True).over(w_csat_distinct_agent_tier_in_2).alias("first_known_next_csat"),
        F.ntile(5).over(w_all_by_priority_csat_max_csa).alias("priority_csat_max_csat_quintile"),
        F.sum("csat").over(w_csat_distinct_agent_tier_in_2).alias("running_csat"),
    )
)

result = (
    sequenced
    .filter(F.col("distinct_agent_id_in_priority_agent_tier") > 2)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "priority_prefix",
        "agent_tier",
        "first_csat",
        "last_known_opened_at",
        "csat_sum_3",
        "csat_share_of_priority_csat",
        "csat_max_1_prior",
        "first_opened_at",
        "priority_csat_max_csat",
        "priority_csat_sum_csat",
        "distinct_agent_id_in_priority_agent_tier",
        "next_csat_2",
        "next_csat_share_of_priority_csat",
        "distinct_agent_id_in_distinct_agent_tier_in_prio",
        "priority_csat_max_csat_percent_rank",
        "first_known_next_csat",
        "running_csat",
        F.concat(F.col("agent_tier"), F.lit("#"), F.col("running_csat").cast("string")).alias("agent_tier_ref"),
    )
    .orderBy("id", F.col("first_csat").desc())
    .limit(20)
)
