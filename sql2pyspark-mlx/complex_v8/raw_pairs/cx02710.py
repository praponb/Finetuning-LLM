from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_at_month_by = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy(F.col("event_at").asc_nulls_last())
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_at_month_by_2 = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_2 = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_rank_by_event_at_ti = Window.partitionBy("event_at_rank").orderBy(F.col("event_at").desc())
w_event_at_rank_second_event_a = Window.partitionBy("event_at_rank", F.to_date("second_event_at_2"))
w_all_by_event_at = Window.orderBy("event_at", "id")

prepared = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 23)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean_2"),
    )
)

sequenced = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_clean",
        "actor_clean_2",
        F.rank().over(w_ticket_id_event_at_month_by).alias("event_at_rank"),
        F.lag("event_at", 2).over(w_ticket_id_by_event_at).alias("prev2_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.round(F.cume_dist().over(w_event_type_by_event_at), 3).alias("event_at_cume_dist_3"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_event_at_month_by_2).alias("last_known_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at_2"),
        F.lag("event_at").over(w_event_type_by_event_at_2).alias("prev_event_at"),
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at_2"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("event_at_rank_2") == 1)
    .drop("event_at_rank_2")
)

sequenced_2 = (
    top_rows
    .select(
        "*",
        F.sum("event_at_cume_dist_3").over(w_all_by_event_at_running).alias("running_event_at_cume_dist_3"),
        F.round(F.percent_rank().over(w_event_at_rank_by_event_at_ti), 3).alias("event_at_percent_rank"),
        F.count(F.lit(1)).over(w_event_at_rank_second_event_a).alias("rows_in_event_at_rank_second_event_at_2_to_date"),
        F.round(F.col("minutes_since_prev_event_at") - F.avg("minutes_since_prev_event_at").over(w_event_at_rank_second_event_a), 2).alias("minutes_since_prev_event_at_vs_event_at_rank_sec"),
        F.lag("prev_event_at").over(w_all_by_event_at).alias("prev_prev_event_at"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank_2"),
        F.round(F.col("event_at_cume_dist_2") - F.avg("event_at_cume_dist_2").over(w_event_at_rank_second_event_a), 2).alias("event_at_cume_dist_2_vs_event_at_rank_second_eve"),
    )
)

result = (
    sequenced_2
    .filter(F.col("event_at_rank_2") <= 9)
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_clean_2",
        "event_at_rank",
        "prev2_event_at",
        "event_at_cume_dist",
        "first_event_at",
        "event_at_cume_dist_2",
        "event_at_cume_dist_3",
        "second_event_at",
        "last_known_event_at",
        "second_event_at_2",
        "prev_event_at",
        "minutes_since_prev_event_at",
        "running_event_at_cume_dist_3",
        "event_at_percent_rank",
        "rows_in_event_at_rank_second_event_at_2_to_date",
        "prev_prev_event_at",
        "event_at_cume_dist_2_vs_event_at_rank_second_eve",
        (F.col("event_at_rank") - F.col("event_at_cume_dist_2")).alias("event_at_rank_minus_event_at_cume_dist_2"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)
