from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_csat_by_ticket_csat = Window.partitionBy("ticket_csat").orderBy("ticket_csat", "id")
w_actor_by_ticket_csat_full = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat = Window.partitionBy("ticket_csat")
w_ticket_csat_by_ticket_csat_f = Window.partitionBy("ticket_csat").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_ticket_csat_ties = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc())
w_ticket_id = Window.partitionBy("ticket_id")
w_event_type_ticket_id = Window.partitionBy("event_type", "ticket_id")
w_event_type_ticket_agent_id_b = Window.partitionBy("event_type", "ticket_agent_id").orderBy(F.col("event_at").desc(), "id")
w_ticket_csat_by_ticket_csat_t = Window.partitionBy("ticket_csat").orderBy("ticket_csat")
w_event_type_ticket_agent_id_b_2 = Window.partitionBy("event_type", "ticket_agent_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(-1, -1)
w_event_type_ticket_agent_id_b_3 = Window.partitionBy("event_type", "ticket_agent_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_event_type_in_ticke = Window.partitionBy("distinct_event_type_in_ticket_id", "last_ticket_csat").orderBy(F.col("ticket_csat_cume_dist").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_event_type_in_ticke_2 = Window.partitionBy("distinct_event_type_in_ticket_id", "last_ticket_csat").orderBy(F.col("ticket_csat_cume_dist").desc_nulls_first())
w_ticket_csat_dense_rank = Window.partitionBy("ticket_csat_dense_rank")
w_all_by_rows_in_ticket_csat_r = Window.orderBy("rows_in_ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_event_type_in_ticke_3 = Window.partitionBy("distinct_event_type_in_ticket_id", "ticket_csat").orderBy(F.col("minutes_since_prev_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_type_ticket_id = Window.orderBy(F.col("event_type_ticket_id_max_ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        "su.agent_name",
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_csat_by_ticket_csat))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.first("ticket_csat").over(w_actor_by_ticket_csat_full).alias("first_ticket_csat"),
        F.round(F.cume_dist().over(w_ticket_csat_by_ticket_csat), 3).alias("ticket_csat_cume_dist"),
        F.count(F.lit(1)).over(w_ticket_csat).alias("rows_in_ticket_csat"),
        F.nth_value("event_at", 2).over(w_ticket_csat_by_ticket_csat_f).alias("second_event_at"),
        F.round(F.cume_dist().over(w_actor_by_ticket_csat_ties), 3).alias("ticket_csat_cume_dist_2"),
        F.size(F.collect_set("event_type").over(w_ticket_id)).alias("distinct_event_type_in_ticket_id"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_event_type_ticket_id), F.lit(0)), 4).alias("ticket_csat_share_of_event_type_ticket_id"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_id), 2).alias("ticket_csat_vs_ticket_id_avg"),
        F.round(F.cume_dist().over(w_event_type_ticket_agent_id_b), 3).alias("event_at_cume_dist"),
        F.max("ticket_csat").over(w_event_type_ticket_id).alias("event_type_ticket_id_max_ticket_csat"),
        F.dense_rank().over(w_ticket_csat_by_ticket_csat_t).alias("ticket_csat_dense_rank"),
        F.max("ticket_csat").over(w_event_type_ticket_agent_id_b_2).alias("ticket_csat_max_1_prior"),
        F.count(F.lit(1)).over(w_ticket_id).alias("rows_in_ticket_id"),
        F.last("ticket_csat").over(w_event_type_ticket_agent_id_b_3).alias("last_ticket_csat"),
    )
)

labeled = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_csat",
        "ticket_agent_id",
        "agent_name",
        "minutes_since_prev_event_at",
        "ticket_csat_cume_dist",
        "rows_in_ticket_csat",
        "ticket_csat_cume_dist_2",
        "distinct_event_type_in_ticket_id",
        "ticket_csat_share_of_event_type_ticket_id",
        "event_at_cume_dist",
        "event_type_ticket_id_max_ticket_csat",
        "ticket_csat_dense_rank",
        "ticket_csat_max_1_prior",
        "rows_in_ticket_id",
        "last_ticket_csat",
        F.least(F.col("rows_in_ticket_id"), F.col("event_at_cume_dist")).alias("least_rows_in_ticket_id_event_at_cume_dist"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("second_event_at")).alias("days_since_second_event_at"),
        F.substring(F.trim(F.col("agent_name")), 1, 4).alias("agent_name_prefix"),
    )
)

with_trends = (
    labeled
    .select(
        "*",
        F.nth_value("ticket_csat_max_1_prior", 2).over(w_distinct_event_type_in_ticke).alias("second_ticket_csat_max_1_prior"),
        F.dense_rank().over(w_distinct_event_type_in_ticke_2).alias("ticket_csat_cume_dist_dense_rank"),
        F.size(F.collect_set("ticket_id").over(w_ticket_csat_dense_rank)).alias("distinct_ticket_id_in_ticket_csat_dense_rank"),
        F.max("ticket_csat_max_1_prior").over(w_all_by_rows_in_ticket_csat_r).alias("max_ticket_csat_max_1_prior_so_far"),
        F.min("last_ticket_csat").over(w_ticket_id).alias("ticket_id_min_last_ticket_csat"),
        F.count(F.lit(1)).over(w_ticket_csat_dense_rank).alias("rows_in_ticket_csat_dense_rank"),
        F.first("ticket_csat_max_1_prior", ignorenulls=True).over(w_distinct_event_type_in_ticke_3).alias("first_known_ticket_csat_max_1_prior"),
        F.nth_value("ticket_csat_max_1_prior", 2).over(w_all_by_event_type_ticket_id).alias("second_ticket_csat_max_1_prior_2"),
    )
)

result = (
    with_trends
    .filter(F.col("agent_name").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_agent_id",
        "agent_name",
        "minutes_since_prev_event_at",
        "ticket_csat_cume_dist",
        "rows_in_ticket_csat",
        "ticket_csat_cume_dist_2",
        "distinct_event_type_in_ticket_id",
        "ticket_csat_share_of_event_type_ticket_id",
        "event_at_cume_dist",
        "event_type_ticket_id_max_ticket_csat",
        "ticket_csat_max_1_prior",
        "last_ticket_csat",
        "least_rows_in_ticket_id_event_at_cume_dist",
        "days_since_second_event_at",
        "agent_name_prefix",
        "ticket_csat_cume_dist_dense_rank",
        "distinct_ticket_id_in_ticket_csat_dense_rank",
        "max_ticket_csat_max_1_prior_so_far",
        "ticket_id_min_last_ticket_csat",
        "rows_in_ticket_csat_dense_rank",
        "first_known_ticket_csat_max_1_prior",
        "second_ticket_csat_max_1_prior_2",
        F.concat_ws(" / ", F.col("agent_name_prefix"), F.col("event_type")).alias("agent_name_prefix_event_type_label"),
        F.abs(F.col("event_at_cume_dist") - 0.6).alias("event_at_cume_dist_distance"),
    )
    .orderBy(F.col("minutes_since_prev_event_at").asc_nulls_last())
)
