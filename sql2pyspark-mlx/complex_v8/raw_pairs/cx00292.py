from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_hour = Window.orderBy("event_at_hour", "id")
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_hour = Window.partitionBy("ticket_id").orderBy("event_at_hour", "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_hour_running = Window.orderBy("event_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_hour_f = Window.partitionBy("ticket_id").orderBy("event_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_ticket_id_event_type_by_even_3 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_hour_tercile_event = Window.partitionBy("event_at_hour_tercile", "event_at_rank").orderBy(F.col("event_at_hour_dense_rank_3").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_at_hour_to_d = Window.partitionBy("ticket_id", F.to_date("event_at_hour"))
w_all_by_event_at_seq_moving_3 = Window.orderBy(F.col("event_at_seq").desc(), "id").rowsBetween(-3, Window.currentRow)

prepared = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
)

sequenced = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_hour",
        F.lead("event_at_hour").over(w_all_by_event_at_hour).alias("next_event_at_hour"),
        F.first("event_at_hour", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("first_known_event_at_hour"),
        F.dense_rank().over(w_ticket_id_by_event_at_hour).alias("event_at_hour_dense_rank"),
        F.rank().over(w_ticket_id_by_event_at).alias("event_at_rank"),
        F.lead("event_at", 3).over(w_ticket_id_by_event_at_hour).alias("next3_event_at"),
        F.ntile(3).over(w_ticket_id_by_event_at_hour).alias("event_at_hour_tercile"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_event_type_by_even).alias("last_known_event_at"),
        F.ntile(5).over(w_ticket_id_by_event_at).alias("event_at_quintile"),
        F.first("event_at_hour", ignorenulls=True).over(w_all_by_event_at_hour_running).alias("first_known_event_at_hour_2"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_hour_f).alias("second_event_at"),
        F.dense_rank().over(w_all_by_event_at_hour).alias("event_at_hour_dense_rank_3"),
        F.row_number().over(w_ticket_id_event_type_by_even_2).alias("event_at_seq"),
        F.nth_value("event_at", 2).over(w_ticket_id_event_type_by_even_3).alias("second_event_at_2"),
        F.ntile(4).over(w_ticket_id_event_type_by_even_2).alias("event_at_quartile"),
    )
)

top_per_group = (
    sequenced
    .filter((F.col("event_at_hour_dense_rank_3") == 1) & (F.col("second_event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")))
)

ranked = (
    top_per_group
    .select(
        "*",
        F.nth_value("event_at_hour_tercile", 2).over(w_event_at_hour_tercile_event).alias("second_event_at_hour_tercile"),
        F.sum("event_at_hour_dense_rank_3").over(w_ticket_id_event_at_hour_to_d).alias("ticket_id_event_at_hour_to_date_sum_event_at_hou"),
        F.round(F.avg("event_at_hour_dense_rank").over(w_all_by_event_at_seq_moving_3), 2).alias("event_at_hour_dense_rank_avg_4"),
    )
)

result = (
    ranked
    .filter(F.col("event_at_hour_dense_rank") <= 7)
    .select(
        "id",
        "event_type",
        "event_at_hour",
        "next_event_at_hour",
        "first_known_event_at_hour",
        "event_at_hour_dense_rank",
        "event_at_rank",
        "event_at_hour_tercile",
        "last_known_event_at",
        "event_at_quintile",
        "event_at_hour_dense_rank_3",
        "event_at_seq",
        "second_event_at_2",
        "event_at_quartile",
        "second_event_at_hour_tercile",
        "ticket_id_event_at_hour_to_date_sum_event_at_hou",
        "event_at_hour_dense_rank_avg_4",
        (F.col("event_at_rank") - F.col("event_at_hour_dense_rank")).alias("event_at_rank_minus_event_at_hour_dense_rank"),
        (F.floor(F.col("event_at_hour_tercile") / 2) * 2).alias("event_at_hour_tercile_bucket"),
    )
    .orderBy(F.col("event_at_rank_minus_event_at_hour_dense_rank").desc())
)
