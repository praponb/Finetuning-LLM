from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_event_at_mont = Window.partitionBy("ticket_channel", F.month("event_at")).orderBy(F.col("event_at").asc_nulls_last())
w_ticket_channel_ticket_id_by = Window.partitionBy("ticket_channel", "ticket_id").orderBy("event_at", "id")
w_ticket_channel_event_at_mont_2 = Window.partitionBy("ticket_channel", F.month("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_priority_event_at_yea = Window.partitionBy("ticket_priority", F.year("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_by_second_event_at = Window.partitionBy("ticket_id").orderBy("second_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_decile_ticket_prior = Window.partitionBy("event_at_decile", "ticket_priority")
w_event_at_quartile_2_event_at = Window.partitionBy("event_at_quartile_2", "event_at_decile").orderBy(F.col("event_at_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_prior_3 = Window.orderBy("event_at", "id").rowsBetween(-3, -1)

valid = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
        "su.agent_name",
    )
)

windowed = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_channel",
        "ticket_priority",
        "agent_name",
        F.dense_rank().over(w_ticket_channel_event_at_mont).alias("event_at_dense_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_channel_ticket_id_by))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(5).over(w_ticket_channel_event_at_mont_2).alias("event_at_quintile"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.ntile(4).over(w_ticket_priority_event_at_yea).alias("event_at_quartile"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.ntile(4).over(w_ticket_channel_ticket_id_by).alias("event_at_quartile_2"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.sum("event_at_quartile").over(w_ticket_id_by_second_event_at).alias("running_event_at_quartile"),
        F.round((F.unix_timestamp("second_event_at") - F.unix_timestamp(F.lag("second_event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_second_event_at"),
        F.size(F.collect_set("actor").over(w_event_at_decile_ticket_prior)).alias("distinct_actor_in_event_at_decile_ticket_priorit"),
        F.nth_value("event_at_dense_rank", 2).over(w_event_at_quartile_2_event_at).alias("second_event_at_dense_rank"),
        F.round(F.avg("event_at_decile").over(w_all_by_event_at_prior_3), 2).alias("event_at_decile_avg_3_prior"),
        F.count(F.lit(1)).over(w_event_at_decile_ticket_prior).alias("rows_in_event_at_decile_ticket_priority"),
        F.size(F.collect_set("ticket_id").over(w_event_at_decile_ticket_prior)).alias("distinct_ticket_id_in_event_at_decile_ticket_pri"),
    )
)

stats = (
    windowed_2
    .groupBy(
        "distinct_ticket_id_in_event_at_decile_ticket_pri",
        "event_at_dense_rank",
        "event_at_quartile",
    )
    .agg(
        F.min("second_event_at_dense_rank").alias("min_second_event_at_dense_rank"),
        F.min("second_event_at").alias("first_second_event_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.sum("event_at_decile_avg_3_prior").alias("sum_event_at_decile_avg_3_prior_check"),
    )
    .filter(F.col("sum_event_at_decile_avg_3_prior_check") > 4.6)
    .drop("sum_event_at_decile_avg_3_prior_check")
)

result = (
    stats
    .select(
        "distinct_ticket_id_in_event_at_decile_ticket_pri",
        "event_at_dense_rank",
        "event_at_quartile",
        "min_second_event_at_dense_rank",
        "first_second_event_at",
        "distinct_ids",
        "actor_list",
        F.abs(F.col("event_at_dense_rank") - 5).alias("event_at_dense_rank_distance"),
    )
    .orderBy("event_at_dense_rank_distance")
)
