from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_runni_2 = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_type_by_event_at_2 = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_seq_prev_event_at_2 = Window.partitionBy("event_at_seq", F.date_trunc("month", "prev_event_at_2")).orderBy(F.col("prev_event_at_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_seq_prev_event_at_2_2 = Window.partitionBy("event_at_seq", F.date_trunc("month", "prev_event_at_2")).orderBy(F.col("prev_event_at_2").desc(), "id")
w_event_at_rank_seq_no = Window.partitionBy("event_at_rank", "seq_no")
w_last_known_event_at_month_by = Window.partitionBy(F.date_trunc("month", "last_known_event_at")).orderBy("event_at_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_events = (
    df_ticket_events
    .filter(~F.col("event_type").isin("reopened", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

ranked = (
    filtered_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("last_known_event_at"),
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni_2).alias("last_known_event_at_2"),
        F.row_number().over(w_event_type_by_event_at).alias("seq_no"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("event_at_seq"),
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at"),
        F.lag("event_at").over(w_event_type_by_event_at_2).alias("prev_event_at_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.rank().over(w_ticket_id_by_event_at).alias("event_at_rank"),
        F.lag("event_at", 3).over(w_ticket_id_by_event_at).alias("prev3_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.last("event_at").over(w_event_type_by_event_at_full).alias("last_event_at"),
    )
)

scored = (
    ranked
    .filter(F.col("seq_no").between(1, 10))
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev3_event_at")).alias("days_since_prev3_event_at"),
        (F.floor(F.col("seq_no") / 2) * 2).alias("seq_no_bucket"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "event_at",
        "last_known_event_at",
        "last_known_event_at_2",
        "seq_no",
        "second_event_at",
        "event_at_seq",
        "prev_event_at",
        "minutes_since_prev_event_at",
        "minutes_since_prev_event_at_2",
        "last_event_at",
        "days_since_prev3_event_at",
        "seq_no_bucket",
        F.sum("days_since_prev3_event_at").over(w_event_at_seq_prev_event_at_2).alias("running_days_since_prev3_event_at"),
        F.lead("days_since_prev3_event_at", 2).over(w_event_at_seq_prev_event_at_2_2).alias("next2_days_since_prev3_event_at"),
        F.row_number().over(w_event_at_seq_prev_event_at_2_2).alias("seq_no_2"),
        F.last("event_at", ignorenulls=True).over(w_event_at_seq_prev_event_at_2).alias("last_known_event_at_3"),
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_event_at_rank_seq_no), 2).alias("seq_no_vs_event_at_rank_seq_no_avg"),
        F.nth_value("prev3_event_at", 2).over(w_last_known_event_at_month_by).alias("second_prev3_event_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "last_known_event_at_2",
        "seq_no",
        "event_at_seq",
        "prev_event_at",
        "minutes_since_prev_event_at_2",
        "last_event_at",
        "days_since_prev3_event_at",
        "running_days_since_prev3_event_at",
        "next2_days_since_prev3_event_at",
        "seq_no_2",
        "last_known_event_at_3",
        "seq_no_vs_event_at_rank_seq_no_avg",
        (F.floor(F.col("seq_no_bucket") / 2) * 2).alias("seq_no_bucket_bucket"),
        F.abs(F.col("event_at_seq") - 6).alias("event_at_seq_distance"),
    )
    .orderBy(F.col("days_since_prev3_event_at").desc_nulls_first())
)
