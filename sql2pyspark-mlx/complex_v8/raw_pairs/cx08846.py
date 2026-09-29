from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_2 = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties_2 = Window.orderBy(F.col("event_at").desc_nulls_first())
w_all_by_event_at_dense_rank_f = Window.orderBy("event_at_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_dense_rank_m = Window.orderBy("event_at_dense_rank", "id").rowsBetween(-2, Window.currentRow)
w_event_at_tercile_event_at_se = Window.partitionBy("event_at_tercile", "event_at_seq")
w_actor_by_event_at_seq_full = Window.partitionBy("actor").orderBy(F.col("event_at_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_last_first_event_at_mon = Window.partitionBy(F.month("last_last_first_event_at"), F.month("event_at_month")).orderBy("cnt")

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.hour(F.col("event_at")).between(8, 13)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.row_number().over(w_all_by_event_at_2).alias("seq_no"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.dense_rank().over(w_all_by_event_at_ties_2).alias("event_at_dense_rank"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
    )
)

leaders = (
    sequenced
    .filter(F.col("seq_no") == 1)
    .drop("seq_no")
)

with_trends = (
    leaders
    .select(
        "*",
        F.first("first_event_at").over(w_all_by_event_at_dense_rank_f).alias("first_first_event_at"),
        F.max("event_at_dense_rank").over(w_all_by_event_at_dense_rank_m).alias("event_at_dense_rank_max_3"),
        F.count(F.lit(1)).over(w_event_at_tercile_event_at_se).alias("rows_in_event_at_tercile_event_at_seq"),
        F.round(F.col("event_at_seq") - F.avg("event_at_seq").over(w_event_at_tercile_event_at_se), 2).alias("event_at_seq_vs_event_at_tercile_event_at_seq_av"),
        F.last("first_event_at").over(w_actor_by_event_at_seq_full).alias("last_first_event_at"),
    )
)

grouped = (
    with_trends
    .filter(F.col("ticket_id").isNotNull())
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"), "ticket_id")
    .agg(
        F.max("event_at_dense_rank_max_3").alias("max_event_at_dense_rank_max_3"),
        F.max("last_first_event_at").alias("last_last_first_event_at"),
        F.count("*").alias("cnt"),
        F.sum("event_at_rank").alias("total_event_at_rank"),
        F.max("event_at_tercile").alias("max_event_at_tercile_check"),
    )
    .filter(F.col("max_event_at_tercile_check") > 2)
    .drop("max_event_at_tercile_check")
)

with_windows = (
    grouped
    .select(
        "*",
        F.round(F.cume_dist().over(w_last_last_first_event_at_mon), 3).alias("cnt_cume_dist"),
        F.rank().over(w_last_last_first_event_at_mon).alias("cnt_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "event_at_month",
        "max_event_at_dense_rank_max_3",
        "last_last_first_event_at",
        "cnt",
        "total_event_at_rank",
        "cnt_cume_dist",
        "cnt_rank",
        F.abs(F.col("cnt") - 7).alias("cnt_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at_month")).alias("days_since_event_at_month"),
    )
    .orderBy(F.col("days_since_event_at_month").desc(), F.col("cnt_rank").asc())
)
