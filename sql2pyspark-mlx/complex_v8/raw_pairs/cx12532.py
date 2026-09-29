from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id")
w_event_at_month_by_event_at_t = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc())
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_event_at = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc(), "id")

filtered = (
    df_ticket_events
    .filter(F.col("event_type") != "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w_actor_by_event_at).alias("seq_no"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.dense_rank().over(w_actor_by_event_at).alias("event_at_dense_rank"),
        F.lead("event_at", 3).over(w_event_at_to_date_by_event_at).alias("next3_event_at"),
        F.rank().over(w_event_at_month_by_event_at_t).alias("event_at_rank_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.ntile(10).over(w_event_at_month_by_event_at).alias("event_at_decile_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_to_date_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_3"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        (F.col("event_at_decile_2") + F.col("event_at_rank_2")).alias("event_at_decile_2_plus_event_at_rank_2"),
        F.when(F.col("event_at_decile_2") >= 7, "A").when(F.col("event_at_decile_2") >= 4, "B").otherwise("C").alias("event_at_decile_2_tier"),
    )
)

result = (
    derived
    .filter(F.col("event_at_rank_2") <= 6)
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "seq_no",
        "event_at_rank",
        "event_at_decile",
        "second_event_at",
        "event_at_percent_rank",
        "event_at_dense_rank",
        "next3_event_at",
        "event_at_rank_2",
        "minutes_since_prev_event_at",
        "event_at_decile_2",
        "minutes_since_prev_event_at_3",
        "event_at_decile_2_plus_event_at_rank_2",
        "event_at_decile_2_tier",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)
