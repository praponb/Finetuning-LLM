from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_group_by_ev = Window.partitionBy("event_type", "actor_group").orderBy(F.col("event_at_plus_1d").desc())
w_event_type_actor_group_by_ev_2 = Window.partitionBy("event_type", "actor_group").orderBy(F.col("event_at_plus_1d").desc(), "id")
w_ticket_id_event_at_plus_1d_m = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at_plus_1d")).orderBy("event_at", "id")
w_ticket_id_event_at_plus_1d_m_2 = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at_plus_1d")).orderBy("event_at")
w_event_at_plus_1d_dense_rank = Window.partitionBy("event_at_plus_1d_dense_rank", "event_type").orderBy(F.col("event_at_plus_1d_dense_rank").desc_nulls_first())
w_event_type_actor_group = Window.partitionBy("event_type", "actor_group")
w_actor_event_at_plus_1d_dense = Window.partitionBy("actor", "event_at_plus_1d_dense_rank").orderBy("event_at_plus_1d_rank", "id").rowsBetween(-6, -1)
w_actor_event_at_plus_1d_dense_2 = Window.partitionBy("actor", "event_at_plus_1d_dense_rank").orderBy("event_at_plus_1d_rank", "id")
w_actor_event_at_month_by_even = Window.partitionBy("actor", F.month("event_at")).orderBy(F.col("event_at").desc(), "id")

valid = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
        F.when(F.col("event_type").isin("replied", "assigned"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_group",
        F.dense_rank().over(w_event_type_actor_group_by_ev).alias("event_at_plus_1d_dense_rank"),
        F.rank().over(w_event_type_actor_group_by_ev_2).alias("event_at_plus_1d_rank"),
        F.round((F.unix_timestamp("event_at_plus_1d") - F.unix_timestamp(F.lag("event_at_plus_1d").over(w_ticket_id_event_at_plus_1d_m))) / 60, 1).alias("minutes_since_prev_event_at_plus_1d"),
        F.round(F.percent_rank().over(w_event_type_actor_group_by_ev_2), 3).alias("event_at_plus_1d_percent_rank"),
        F.round(F.cume_dist().over(w_ticket_id_event_at_plus_1d_m_2), 3).alias("event_at_cume_dist"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_group",
        "event_at_plus_1d_rank",
        "minutes_since_prev_event_at_plus_1d",
        "event_at_plus_1d_percent_rank",
        "event_at_cume_dist",
        F.round(F.cume_dist().over(w_event_at_plus_1d_dense_rank), 3).alias("event_at_plus_1d_dense_rank_cume_dist"),
        F.count(F.lit(1)).over(w_event_type_actor_group).alias("rows_in_event_type_actor_group"),
        F.sum("event_at_cume_dist").over(w_actor_event_at_plus_1d_dense).alias("event_at_cume_dist_sum_6_prior"),
        F.lead("event_at").over(w_actor_event_at_plus_1d_dense_2).alias("next_event_at"),
        F.lag("event_at_plus_1d_dense_rank", 2).over(w_actor_event_at_month_by_even).alias("prev2_event_at_plus_1d_dense_rank"),
        F.rank().over(w_event_at_plus_1d_dense_rank).alias("event_at_plus_1d_dense_rank_rank"),
    )
)

first_rows = with_windows.filter(F.col("event_at_plus_1d_rank") <= 5)

line_level = (
    first_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_group",
        "s.minutes_since_prev_event_at_plus_1d",
        "s.event_at_cume_dist",
        "s.event_at_plus_1d_dense_rank_cume_dist",
        "s.event_at_cume_dist_sum_6_prior",
        "s.next_event_at",
        "s.prev2_event_at_plus_1d_dense_rank",
        "s.event_at_plus_1d_dense_rank_rank",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

annotated = (
    line_level
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_group",
        "event_at_cume_dist",
        "event_at_plus_1d_dense_rank_cume_dist",
        "prev2_event_at_plus_1d_dense_rank",
        "event_id",
        "event_event_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("next_event_at")).alias("days_since_next_event_at"),
    )
)

result = (
    annotated
    .filter(F.col("actor_group") == "group_b")
    .select(
        "id",
        "actor",
        "actor_group",
        "event_at_cume_dist",
        "event_at_plus_1d_dense_rank_cume_dist",
        "event_event_at",
        "days_since_next_event_at",
    )
    .orderBy(F.col("event_at_cume_dist").asc_nulls_last())
)
