from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first())
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_actor_or_unknown_ticket_id_b = Window.partitionBy("actor_or_unknown", "ticket_id").orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_or_unknown_ticket_id_b_2 = Window.partitionBy("actor_or_unknown", "ticket_id").orderBy("event_at")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_decile_actor_or_unk = Window.partitionBy("event_at_decile", "actor_or_unknown").orderBy(F.col("second_event_at").desc(), "id")
w_event_at_decile = Window.partitionBy("event_at_decile")

recent = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) | (F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lag("event_at").over(w_event_type_by_event_at).alias("prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_or_unknown_ticket_id_b))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.dense_rank().over(w_actor_or_unknown_ticket_id_b_2).alias("event_at_dense_rank"),
        F.row_number().over(w_event_type_by_event_at).alias("rn"),
        F.ntile(10).over(w_actor_or_unknown_ticket_id_b).alias("event_at_decile"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.first("event_at").over(w_event_type_by_event_at_full).alias("first_event_at_2"),
        F.dense_rank().over(w_actor_or_unknown_ticket_id_b).alias("event_at_dense_rank_2"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_or_unknown",
        "first_known_event_at",
        "event_at_cume_dist",
        "prev_event_at",
        "minutes_since_prev_event_at",
        "event_at_rank",
        "second_event_at",
        "event_at_dense_rank",
        "first_event_at",
        "first_event_at_2",
        "event_at_dense_rank_2",
        F.ntile(3).over(w_event_at_decile_actor_or_unk).alias("second_event_at_tercile"),
        F.round(F.col("event_at_rank") / F.nullif(F.sum("event_at_rank").over(w_event_at_decile), F.lit(0)), 4).alias("event_at_rank_share_of_event_at_decile"),
        F.max("event_at_rank").over(w_event_at_decile).alias("event_at_decile_max_event_at_rank"),
    )
)

result = (
    metrics
    .filter(F.col("first_event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_or_unknown",
        "event_at_cume_dist",
        "prev_event_at",
        "minutes_since_prev_event_at",
        "event_at_rank",
        "second_event_at",
        "event_at_dense_rank",
        "first_event_at",
        "event_at_dense_rank_2",
        "second_event_at_tercile",
        "event_at_rank_share_of_event_at_decile",
        "event_at_decile_max_event_at_rank",
        F.coalesce(F.col("event_at_cume_dist"), F.lit(0.0)).alias("event_at_cume_dist_filled"),
        (F.col("event_at_rank") + F.col("second_event_at_tercile")).alias("event_at_rank_plus_second_event_at_tercile"),
    )
    .orderBy(
        F.col("first_event_at").desc_nulls_first(),
        F.col("actor_or_unknown").asc(),
    )
)
