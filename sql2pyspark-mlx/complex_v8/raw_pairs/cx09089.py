from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc())
w_event_type_actor = Window.partitionBy("event_type", "actor")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_prior_6 = Window.orderBy("event_at", "id").rowsBetween(-6, -1)
w_all_by_event_at_moving_2 = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_prev_event_at_month = Window.partitionBy(F.month("prev_event_at"))
w_all_by_rows_in_event_type_ac = Window.orderBy(F.col("rows_in_event_type_actor_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_type_actor_max = Window.orderBy("event_type_actor_max_event_at_quarter")
w_all_by_event_at_cume_dist_2 = Window.orderBy(F.col("event_at_cume_dist_2").desc(), "id")

filtered_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

with_trends = (
    filtered_events
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.cume_dist().over(w_event_type_actor_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.round(F.col("event_at_quarter") / F.nullif(F.sum("event_at_quarter").over(w_event_type_actor), F.lit(0)), 4).alias("event_at_quarter_share_of_event_type_actor"),
        F.lag("event_at", 2).over(w_event_type_actor_by_event_at_2).alias("prev2_event_at"),
        F.count(F.lit(1)).over(w_event_type_actor_by_event_at_3).alias("running_count"),
        F.size(F.collect_set("ticket_id").over(w_event_type_actor)).alias("distinct_ticket_id_in_event_type_actor"),
        F.count(F.lit(1)).over(w_event_type_actor).alias("rows_in_event_type_actor"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.lead("event_at").over(w_event_type_actor_by_event_at_2).alias("next_event_at"),
        F.max("event_at_quarter").over(w_event_type_actor).alias("event_type_actor_max_event_at_quarter"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.sum("event_at_quarter").over(w_all_by_event_at_prior_6).alias("event_at_quarter_sum_6_prior"),
        F.round(F.avg("event_at_quarter").over(w_all_by_event_at_moving_2), 2).alias("event_at_quarter_avg_3"),
    )
)

enriched = (
    with_trends
    .filter(F.col("event_at_cume_dist") >= 0.3)
    .select(
        "id",
        "event_at",
        "actor",
        "event_at_quarter",
        "event_at_cume_dist",
        "event_at_cume_dist_2",
        "event_at_quarter_share_of_event_type_actor",
        "prev2_event_at",
        "running_count",
        "prev_event_at",
        "event_type_actor_max_event_at_quarter",
        "last_event_at",
        F.round(F.col("event_at_cume_dist_2") / F.nullif(F.col("event_at_quarter_avg_3"), F.lit(0)), 3).alias("event_at_cume_dist_2_per_event_at_quarter_avg_3"),
        F.abs(F.col("rows_in_event_type_actor") - 8).alias("rows_in_event_type_actor_distance"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("actor").over(w_prev_event_at_month)).alias("distinct_actor_in_prev_event_at_month"),
        F.nth_value("prev_event_at", 2).over(w_all_by_rows_in_event_type_ac).alias("second_prev_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_type_actor_max), 3).alias("event_type_actor_max_event_at_quarter_cume_dist"),
        F.round(F.avg("event_at_cume_dist_2").over(w_prev_event_at_month), 2).alias("prev_event_at_month_avg_event_at_cume_dist_2"),
        F.row_number().over(w_all_by_event_at_cume_dist_2).alias("event_at_cume_dist_2_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "event_at",
        "actor",
        "event_at_quarter",
        "event_at_quarter_share_of_event_type_actor",
        "prev2_event_at",
        "prev_event_at",
        "event_type_actor_max_event_at_quarter",
        "last_event_at",
        "rows_in_event_type_actor_distance",
        "distinct_actor_in_prev_event_at_month",
        "second_prev_event_at",
        "event_type_actor_max_event_at_quarter_cume_dist",
        "event_at_cume_dist_2_seq",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.round((F.unix_timestamp(F.col("prev_event_at")) - F.unix_timestamp(F.col("event_at"))) / 60, 1).alias("minutes_event_at_to_prev_event_at"),
    )
    .orderBy("event_type_actor_max_event_at_quarter")
)
