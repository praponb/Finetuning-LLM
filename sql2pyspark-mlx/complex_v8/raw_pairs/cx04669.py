from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_actor_ref_len_running = Window.orderBy("actor_ref_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_actor_ref_len_full = Window.orderBy("actor_ref_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type = Window.partitionBy("event_type")
w_ticket_id_by_event_at_hour_t = Window.partitionBy("ticket_id").orderBy(F.col("event_at_hour").desc())
w_ticket_id_by_event_at_hour_r = Window.partitionBy("ticket_id").orderBy(F.col("event_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 50)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

annotated = (
    scoped
    .filter(F.col("event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

labeled = (
    annotated
    .select(
        "*",
        F.length(F.trim(F.col("actor_ref"))).alias("actor_ref_len"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.concat_ws("|", F.col("event_type"), F.col("event_at_label")).alias("event_type_event_at_label_label"),
    )
)

windowed = (
    labeled
    .select(
        "*",
        F.min("actor_ref_len").over(w_all_by_actor_ref_len_running).alias("min_actor_ref_len_so_far"),
        F.nth_value("event_at", 2).over(w_all_by_actor_ref_len_full).alias("second_event_at"),
        F.sum("actor_ref_len").over(w_event_type).alias("event_type_sum_actor_ref_len"),
        F.dense_rank().over(w_ticket_id_by_event_at_hour_t).alias("event_at_hour_dense_rank"),
        F.last("actor_ref_len", ignorenulls=True).over(w_ticket_id_by_event_at_hour_r).alias("last_known_actor_ref_len"),
    )
)

labeled_2 = (
    windowed
    .select(
        "id",
        "actor",
        "actor_ref",
        "event_at_label",
        "actor_ref_len",
        "event_at_hour",
        "min_actor_ref_len_so_far",
        "event_at_hour_dense_rank",
        F.round((F.unix_timestamp(F.col("second_event_at")) - F.unix_timestamp(F.col("event_at_hour"))) / 3600, 1).alias("hours_event_at_hour_to_second_event_at"),
    )
)

result = (
    labeled_2
    .select(
        "id",
        "actor",
        "actor_ref",
        "event_at_label",
        "actor_ref_len",
        "event_at_hour",
        "min_actor_ref_len_so_far",
        "event_at_hour_dense_rank",
        "hours_event_at_hour_to_second_event_at",
        F.coalesce(F.col("actor_ref_len"), F.lit(0)).alias("actor_ref_len_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("actor_ref")), " ", "_")).alias("actor_ref_slug"),
    )
    .orderBy(
        F.col("actor_ref_slug").desc_nulls_first(),
        F.col("event_at_label").desc(),
    )
)
