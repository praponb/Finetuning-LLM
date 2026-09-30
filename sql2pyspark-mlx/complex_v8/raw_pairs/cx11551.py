from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last())
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date = Window.partitionBy(F.to_date("event_at"))
w_event_at_dense_rank_event_ty = Window.partitionBy("event_at_dense_rank", "event_type").orderBy(F.col("prev2_event_at").desc_nulls_first(), "id")

prepared = (
    df_ticket_events
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    prepared
    .select(
        "id",
        "event_at",
        "event_type",
        F.lag("event_at", 2).over(w_all_by_event_at).alias("prev2_event_at"),
        F.dense_rank().over(w_event_type_by_event_at_ties).alias("event_at_dense_rank"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.round(F.col("event_at_dense_rank") - F.avg("event_at_dense_rank").over(w_event_at_to_date), 2).alias("event_at_dense_rank_vs_event_at_to_date_avg"),
        F.round((F.unix_timestamp("prev2_event_at") - F.unix_timestamp(F.lag("prev2_event_at").over(w_event_at_dense_rank_event_ty))) / 60, 1).alias("minutes_since_prev_prev2_event_at"),
        F.lag("event_at").over(w_event_at_dense_rank_event_ty).alias("prev_event_at"),
        F.size(F.collect_set("event_type").over(w_event_at_to_date)).alias("distinct_event_type_in_event_at_to_date"),
    )
)

annotated = (
    sequenced
    .select(
        "*",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        (F.floor(F.col("event_at_dense_rank") / 2) * 2).alias("event_at_dense_rank_bucket"),
    )
)

result = (
    annotated
    .filter(F.col("minutes_since_prev_prev2_event_at") <= 3000.0)
    .select(
        "id",
        "event_at",
        "event_type",
        "prev2_event_at",
        "event_at_dense_rank",
        "last_known_event_at",
        "event_at_dense_rank_vs_event_at_to_date_avg",
        "minutes_since_prev_prev2_event_at",
        "prev_event_at",
        "distinct_event_type_in_event_at_to_date",
        "event_at_label",
        "event_at_dense_rank_bucket",
        F.concat(F.col("event_type"), F.lit("#"), F.col("event_at_dense_rank").cast("string")).alias("event_type_ref"),
    )
    .orderBy(F.col("minutes_since_prev_prev2_event_at").desc())
)
