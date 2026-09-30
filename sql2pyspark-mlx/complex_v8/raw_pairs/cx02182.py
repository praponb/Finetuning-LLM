from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_rank_actor_group_by = Window.partitionBy("event_at_rank", "actor_group").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_group_ticket_id = Window.partitionBy("actor_group", "ticket_id")
w_event_at_rank_actor_group_by_2 = Window.partitionBy("event_at_rank", "actor_group").orderBy(F.col("event_at").asc_nulls_last())
w_event_at_rank_actor_group_by_3 = Window.partitionBy("event_at_rank", "actor_group").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_event_at_rank_by_event_at_de = Window.partitionBy("event_at_rank").orderBy("event_at_dense_rank")
w_actor_actor_group = Window.partitionBy("actor", "actor_group")
w_actor_group_by_next_event_at = Window.partitionBy("actor_group").orderBy("next_event_at_rank", "id")
w_actor_group_actor_by_event_a = Window.partitionBy("actor_group", "actor").orderBy("event_at_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_rank_event_at_to_da = Window.partitionBy("event_at_rank", F.to_date("event_at"))
w_actor_group_actor_by_event_a_2 = Window.partitionBy("actor_group", "actor").orderBy("event_at_rank")
w_all_by_event_at_dense_rank_p = Window.orderBy(F.col("event_at_dense_rank_percent_rank").desc(), "id").rowsBetween(-3, Window.currentRow)
w_actor_group_by_next_event_at_2 = Window.partitionBy("actor_group").orderBy("next_event_at_rank", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(6, 8))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

metrics = (
    base
    .withColumn("event_at_rank", F.rank().over(w_all_by_event_at))
    .withColumn("first_known_event_at", F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running))
)

with_trends = (
    metrics
    .select(
        "*",
        F.lead("event_at_rank", 1, 0).over(w_event_at_rank_actor_group_by).alias("next_event_at_rank"),
        F.round(F.avg("event_at_rank").over(w_actor_group_ticket_id), 2).alias("actor_group_ticket_id_avg_event_at_rank"),
        F.dense_rank().over(w_event_at_rank_actor_group_by_2).alias("event_at_dense_rank"),
        F.sum("event_at_rank").over(w_event_at_rank_actor_group_by_3).alias("event_at_rank_sum_3"),
    )
)

windowed = (
    with_trends
    .withColumn("event_at_dense_rank_percent_rank", F.round(F.percent_rank().over(w_event_at_rank_by_event_at_de), 3))
    .withColumn("event_at_rank_sum_3_vs_actor_actor_group_avg", F.round(F.col("event_at_rank_sum_3") - F.avg("event_at_rank_sum_3").over(w_actor_actor_group), 2))
)

qualified = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") <= 23), "left_semi")
)

sequenced = (
    qualified
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor_group",
        "event_at_rank",
        "first_known_event_at",
        "actor_group_ticket_id_avg_event_at_rank",
        "event_at_rank_sum_3_vs_actor_actor_group_avg",
        F.lag("event_at_dense_rank").over(w_actor_group_by_next_event_at).alias("prev_event_at_dense_rank"),
        F.min("event_at_rank_sum_3").over(w_actor_group_actor_by_event_a).alias("min_event_at_rank_sum_3_so_far"),
        F.count(F.lit(1)).over(w_event_at_rank_event_at_to_da).alias("rows_in_event_at_rank_event_at_to_date"),
        F.round(F.cume_dist().over(w_actor_group_actor_by_event_a_2), 3).alias("event_at_rank_cume_dist"),
        F.round(F.avg("next_event_at_rank").over(w_all_by_event_at_dense_rank_p), 2).alias("next_event_at_rank_avg_4"),
        F.round(F.avg("next_event_at_rank").over(w_actor_group_by_next_event_at_2), 2).alias("next_event_at_rank_avg_4_2"),
    )
)

enriched_ticket = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") < 16), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor_group",
        "s.event_at_rank",
        "s.actor_group_ticket_id_avg_event_at_rank",
        "s.prev_event_at_dense_rank",
        "s.event_at_rank_cume_dist",
        "s.next_event_at_rank_avg_4_2",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    enriched_ticket
    .groupingSets(
        [["ticket_channel", "agent_tier", "actor_group"], ["ticket_channel"]],
        "ticket_channel", "agent_tier", "actor_group",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("event_at").alias("last_event_at"),
        F.sum(F.when(F.col("ticket_channel").isin("phone", "web"), F.col("actor_group_ticket_id_avg_event_at_rank"))).alias("actor_group_ticket_id_avg_event_at_rank_ticket_c"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .orderBy(
        F.col("ticket_channel").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
        F.col("actor_group").asc_nulls_last(),
    )
)
