from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_csat_by_opened_at_f = Window.partitionBy("priority", "csat").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_group_priority = Window.partitionBy("channel_group", "priority")
w_all_by_opened_at_prior_2 = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(-2, -1)
w_csat_by_csat_full = Window.partitionBy("csat").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_next3_csat = Window.partitionBy("next3_csat")
w_all_by_csat_avg_2_prior_prio = Window.orderBy(F.col("csat_avg_2_prior").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_csat_avg_2_prior_full = Window.orderBy(F.col("csat_avg_2_prior").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_avg_2_prior = Window.orderBy(F.col("csat_avg_2_prior").asc_nulls_last(), "id")

cleaned = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "refund")) | (F.col("csat") >= 2))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_ref"),
        F.when(F.col("channel").isin("phone", "web"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_group",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        "channel_group",
        "agent_name",
        F.nth_value("opened_at", 2).over(w_priority_csat_by_opened_at_f).alias("second_opened_at"),
        F.max("csat").over(w_channel_group_priority).alias("channel_group_priority_max_csat"),
        F.round(F.avg("csat").over(w_all_by_opened_at_prior_2), 2).alias("csat_avg_2_prior"),
        F.count(F.lit(1)).over(w_channel_group_priority).alias("rows_in_channel_group_priority"),
        F.size(F.collect_set("channel").over(w_channel_group_priority)).alias("distinct_channel_in_channel_group_priority"),
        F.nth_value("opened_at", 2).over(w_csat_by_csat_full).alias("second_opened_at_2"),
        F.round(F.avg("csat").over(w_channel_group_priority), 2).alias("channel_group_priority_avg_csat"),
        F.sum("csat").over(w_channel_group_priority).alias("channel_group_priority_sum_csat"),
        F.lead("csat", 3).over(w_all_by_opened_at).alias("next3_csat"),
    )
)

annotated = (
    ranked
    .filter(F.col("opened_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        "channel_group",
        "agent_name",
        "second_opened_at",
        "channel_group_priority_max_csat",
        "csat_avg_2_prior",
        "rows_in_channel_group_priority",
        "channel_group_priority_sum_csat",
        "next3_csat",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("second_opened_at")).alias("days_since_second_opened_at"),
        F.array_contains(F.col("tags"), "outage").alias("has_outage"),
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
    )
)

ranked_2 = (
    annotated
    .select(
        "*",
        F.min("csat_avg_2_prior").over(w_next3_csat).alias("next3_csat_min_csat_avg_2_prior"),
        F.max("csat_avg_2_prior").over(w_all_by_csat_avg_2_prior_prio).alias("csat_avg_2_prior_max_2_prior"),
        F.nth_value("second_opened_at", 2).over(w_all_by_csat_avg_2_prior_full).alias("second_second_opened_at"),
        F.lead("days_since_second_opened_at", 2).over(w_all_by_csat_avg_2_prior).alias("next2_days_since_second_opened_at"),
        F.sum("csat_avg_2_prior").over(w_next3_csat).alias("next3_csat_sum_csat_avg_2_prior"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "agent_id",
        "channel",
        "channel_group",
        "agent_name",
        "second_opened_at",
        "channel_group_priority_max_csat",
        "csat_avg_2_prior",
        "rows_in_channel_group_priority",
        "channel_group_priority_sum_csat",
        "days_since_second_opened_at",
        "has_outage",
        "opened_at_year",
        "next3_csat_min_csat_avg_2_prior",
        "csat_avg_2_prior_max_2_prior",
        "second_second_opened_at",
        "next2_days_since_second_opened_at",
        "next3_csat_sum_csat_avg_2_prior",
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.coalesce(F.col("next3_csat"), F.lit(0)).alias("next3_csat_filled"),
    )
    .orderBy(F.col("rows_in_channel_group_priority").desc(), "id")
    .limit(20)
)
