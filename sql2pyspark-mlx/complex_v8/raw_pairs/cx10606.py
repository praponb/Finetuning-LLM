from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_event_type_event_at_dayofwee = Window.partitionBy("event_type", "event_at_dayofweek").orderBy(F.col("event_at").desc())
w_all_by_event_at_dayofweek = Window.orderBy(F.col("event_at_dayofweek").desc(), "id")
w_event_at_dayofweek_rank = Window.partitionBy("event_at_dayofweek_rank")
w_actor_event_at_dayofweek_by = Window.partitionBy("actor", "event_at_dayofweek").orderBy("event_at_dayofweek_rank")

prepared = (
    df_ticket_events
    .filter((F.col("event_type") != "replied") & (~F.col("actor").isin("agent", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_event_type_label",
        "event_at_dayofweek",
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.round(F.percent_rank().over(w_event_type_event_at_dayofwee), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_all_by_event_at_dayofweek).alias("next_event_at"),
        F.rank().over(w_all_by_event_at_dayofweek).alias("event_at_dayofweek_rank"),
        F.sum("event_at_dayofweek").over(w_event_type).alias("event_type_sum_event_at_dayofweek"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("event_at_dayofweek_rank") <= 2) & (F.col("event_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")))
)

labeled = (
    top_per_group
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.round((F.unix_timestamp(F.col("next_event_at")) - F.unix_timestamp(F.col("event_at"))) / 60, 1).alias("minutes_event_at_to_next_event_at"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.round(F.avg("days_since_event_at").over(w_event_at_dayofweek_rank), 2).alias("event_at_dayofweek_rank_avg_days_since_event_at"),
        F.round(F.avg("rows_in_event_type").over(w_event_at_dayofweek_rank), 2).alias("event_at_dayofweek_rank_avg_rows_in_event_type"),
        F.min("event_at_percent_rank").over(w_event_at_dayofweek_rank).alias("event_at_dayofweek_rank_min_event_at_percent_ran"),
        F.round(F.cume_dist().over(w_actor_event_at_dayofweek_by), 3).alias("event_at_dayofweek_rank_cume_dist"),
        F.size(F.collect_set("ticket_id").over(w_event_at_dayofweek_rank)).alias("distinct_ticket_id_in_event_at_dayofweek_rank"),
        F.size(F.collect_set("actor").over(w_event_at_dayofweek_rank)).alias("distinct_actor_in_event_at_dayofweek_rank"),
    )
)

with_children = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("system", "agent")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_event_type_label",
        "s.rows_in_event_type",
        "s.next_event_at",
        "s.event_at_dayofweek_rank",
        "s.minutes_event_at_to_next_event_at",
        "s.event_at_dayofweek_rank_min_event_at_percent_ran",
        "s.event_at_dayofweek_rank_cume_dist",
        "s.distinct_actor_in_event_at_dayofweek_rank",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

labeled_2 = (
    with_children
    .filter(F.col("event_actor") != "customer")
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("next_event_at")).alias("days_since_next_event_at"),
        F.abs(F.col("event_at_dayofweek_rank") - 5).alias("event_at_dayofweek_rank_distance"),
    )
)

result = (
    labeled_2
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_event_type_label",
        "rows_in_event_type",
        "next_event_at",
        "event_at_dayofweek_rank",
        "event_at_dayofweek_rank_cume_dist",
        "distinct_actor_in_event_at_dayofweek_rank",
        "event_id",
        "days_since_next_event_at",
        "event_at_dayofweek_rank_distance",
    )
    .orderBy(F.col("days_since_next_event_at").asc_nulls_last())
)
