from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev_event_at_full = Window.orderBy(F.col("prev_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prev_event_at_prior_1 = Window.orderBy(F.col("prev_event_at").desc(), "id").rowsBetween(-1, -1)
w_latest = Window.partitionBy("event_type").orderBy(F.col("event_type_len_avg_1_prior").desc(), F.col("id").desc())

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_ticket_id_running))

metrics = (
    sessionized
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_len",
        "prev_event_at",
        "session_id",
        F.last("starts_session").over(w_all_by_prev_event_at_full).alias("last_starts_session"),
        F.round(F.avg("event_type_len").over(w_all_by_prev_event_at_prior_1), 2).alias("event_type_len_avg_1_prior"),
    )
)

latest_per_key = (
    metrics
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "event_type",
        "event_type_len",
        "session_id",
        "last_starts_session",
        "event_type_len_avg_1_prior",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label_2"),
    )
    .orderBy(F.col("last_starts_session").desc(), "event_at_label", "id")
    .limit(10)
)
