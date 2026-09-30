from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_week = Window.partitionBy("actor").orderBy("event_at_week", "id")
w_ticket_priority_event_type_b = Window.partitionBy("ticket_priority", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(-6, -1)
w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_priority_event_type_b_2 = Window.partitionBy("ticket_priority", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_actor_len_buck = Window.orderBy(F.col("second_actor_len_bucket").desc(), "id")
w_event_at_week_to_date_second = Window.partitionBy(F.to_date("event_at_week"), "second_actor_len_bucket").orderBy("event_at", "id")
w_event_at_week_to_date_second_2 = Window.partitionBy(F.to_date("event_at_week"), "second_actor_len_bucket").orderBy("event_at", "id").rowsBetween(-2, -1)

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

derived = (
    cleaned
    .filter(F.col("event_type").isin("escalated", "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "event_at_label",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        (F.floor(F.col("actor_len") / 2) * 2).alias("actor_len_bucket"),
    )
)

with_parent_ticket = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    with_parent_ticket
    .select(
        "*",
        F.ntile(5).over(w_actor_by_event_at_week).alias("event_at_week_quintile"),
        F.max("actor_len_bucket").over(w_ticket_priority_event_type_b).alias("actor_len_bucket_max_6_prior"),
        F.size(F.collect_set("agent_tier").over(w_ticket_id)).alias("distinct_agent_tier_in_ticket_id"),
        F.nth_value("actor_len_bucket", 2).over(w_ticket_priority_event_type_b_2).alias("second_actor_len_bucket"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_week",
        "ticket_agent_id",
        "ticket_tags",
        "agent_tier",
        "actor_len_bucket_max_6_prior",
        "distinct_agent_tier_in_ticket_id",
        "second_actor_len_bucket",
        F.dense_rank().over(w_all_by_second_actor_len_buck).alias("second_actor_len_bucket_dense_rank"),
        F.lead("event_at").over(w_event_at_week_to_date_second).alias("next_event_at"),
        F.round(F.avg("actor_len_bucket_max_6_prior").over(w_event_at_week_to_date_second_2), 2).alias("actor_len_bucket_max_6_prior_avg_2_prior"),
    )
)

result = (
    sequenced
    .filter(F.hour(F.col("event_at")).between(10, 12))
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_week",
        "ticket_tags",
        "agent_tier",
        "actor_len_bucket_max_6_prior",
        "distinct_agent_tier_in_ticket_id",
        "second_actor_len_bucket",
        "second_actor_len_bucket_dense_rank",
        "next_event_at",
        "actor_len_bucket_max_6_prior_avg_2_prior",
        F.coalesce(F.col("second_actor_len_bucket"), F.lit(0)).alias("second_actor_len_bucket_filled"),
        F.year(F.col("event_at_week")).alias("event_at_week_year"),
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("event_at_week_year").desc())
)
