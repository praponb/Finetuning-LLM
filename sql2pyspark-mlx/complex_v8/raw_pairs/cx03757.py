from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month = Window.partitionBy(F.month("event_at"))
w_ticket_id_by_ticket_requeste = Window.partitionBy("ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ticket_opened_at = Window.orderBy(F.col("ticket_opened_at").asc_nulls_last(), "id")
w_actor = Window.partitionBy("actor")
w_agent_tier_ticket_opened_at = Window.partitionBy("agent_tier", F.year("ticket_opened_at")).orderBy("event_at", "id")
w_agent_tier_ticket_opened_at_2 = Window.partitionBy("agent_tier", F.year("ticket_opened_at")).orderBy("event_at", "id").rowsBetween(-6, -1)

recent_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-01-31").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

joined = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_type_actor_label",
        "s.event_type_ref",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.round(F.avg("ticket_requester_id").over(w_event_at_month), 2).alias("event_at_month_avg_ticket_requester_id"),
        F.sum("ticket_requester_id").over(w_event_at_month).alias("event_at_month_sum_ticket_requester_id"),
        F.last("ticket_requester_id", ignorenulls=True).over(w_ticket_id_by_ticket_requeste).alias("last_known_ticket_requester_id"),
        F.row_number().over(w_all_by_ticket_opened_at).alias("seq_no"),
        F.size(F.collect_set("agent_tier").over(w_actor)).alias("distinct_agent_tier_in_actor"),
        F.rank().over(w_all_by_ticket_opened_at).alias("ticket_opened_at_rank"),
        F.size(F.collect_set("ticket_id").over(w_actor)).alias("distinct_ticket_id_in_actor"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_agent_tier_ticket_opened_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_actor), F.lit(0)), 4).alias("ticket_requester_id_share_of_actor"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_actor), 2).alias("ticket_requester_id_vs_actor_avg"),
        F.max("ticket_requester_id").over(w_agent_tier_ticket_opened_at_2).alias("ticket_requester_id_max_6_prior"),
        F.max("ticket_requester_id").over(w_actor).alias("actor_max_ticket_requester_id"),
        F.lag("ticket_opened_at").over(w_agent_tier_ticket_opened_at).alias("prev_ticket_opened_at"),
    )
)

summary = (
    with_trends
    .groupBy(
        "ticket_id",
        "distinct_agent_tier_in_actor",
        "distinct_ticket_id_in_actor",
    )
    .agg(
        F.sum(F.when(F.col("actor").isin("agent", "system"), F.col("ticket_requester_id_max_6_prior"))).alias("ticket_requester_id_max_6_prior_actor_in_agent_s"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.sum(F.when(F.col("actor").isNotNull(), 1).otherwise(0)).alias("n_actor_is_not_null"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("n_actor_is_not_null_2"),
    )
)

result = (
    summary
    .select(
        "ticket_id",
        "distinct_agent_tier_in_actor",
        "ticket_requester_id_max_6_prior_actor_in_agent_s",
        "actor_list",
    )
    .orderBy(
        "ticket_requester_id_max_6_prior_actor_in_agent_s",
        F.col("distinct_agent_tier_in_actor").asc_nulls_last(),
    )
)
