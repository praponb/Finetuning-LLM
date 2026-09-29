from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_ticket_id_actor_by_event_at_3 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").asc_nulls_last())
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_second_event_at_to_date = Window.partitionBy(F.to_date("second_event_at"))
w_second_event_at_2_to_date_ev = Window.partitionBy(F.to_date("second_event_at_2"), "event_type").orderBy("minutes_since_prev_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 24) & (F.col("event_type") == "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    base_events
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_ticket_id_actor_by_event_at).alias("second_event_at"),
        F.row_number().over(w_ticket_id_actor_by_event_at_2).alias("rn"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at_3), 3).alias("event_at_cume_dist"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist_2"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.last("event_at").over(w_ticket_id_actor_by_event_at).alias("last_event_at"),
        F.lead("event_at").over(w_ticket_id_actor_by_event_at_2).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at_2"),
        F.row_number().over(w_ticket_id_by_event_at).alias("rn_2"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.max("event_at_cume_dist").over(w_second_event_at_to_date).alias("second_event_at_to_date_max_event_at_cume_dist"),
        F.round(F.col("event_at_percent_rank") - F.avg("event_at_percent_rank").over(w_second_event_at_to_date), 2).alias("event_at_percent_rank_vs_second_event_at_to_date"),
        F.round(F.col("rn_2") - F.avg("rn_2").over(w_second_event_at_to_date), 2).alias("rn_2_vs_second_event_at_to_date_avg"),
        F.nth_value("minutes_since_prev_event_at", 2).over(w_second_event_at_2_to_date_ev).alias("second_minutes_since_prev_event_at"),
    )
)

stats = (
    sequenced_2
    .groupBy("rn", F.year("next_event_at").alias("next_event_at_year"), "actor")
    .agg(
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.max("event_at").alias("last_event_at_2"),
        F.sum("minutes_since_prev_event_at").alias("total_minutes_since_prev_event_at"),
        F.sum("event_at_cume_dist").alias("sum_event_at_cume_dist_check"),
    )
    .filter(F.col("sum_event_at_cume_dist_check") > 0.4)
    .drop("sum_event_at_cume_dist_check")
)

result = (
    stats
    .filter(F.col("distinct_ticket_ids") > 4)
    .select(
        "rn",
        "next_event_at_year",
        "actor",
        "distinct_ticket_ids",
        "last_event_at_2",
        "total_minutes_since_prev_event_at",
    )
    .orderBy(
        F.col("last_event_at_2").desc(),
        "rn",
        "next_event_at_year",
        "actor",
    )
    .limit(5)
)
