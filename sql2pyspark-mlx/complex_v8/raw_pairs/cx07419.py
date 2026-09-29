from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 15)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

grouped = (
    sessions
    .groupBy(
        "event_type",
        F.to_date("session_end").alias("session_end_day"),
        "burst_id",
    )
    .agg(
        F.min("n_events").alias("min_n_events"),
        F.sum("n_events").alias("total_n_events"),
        F.min("duration_min").alias("min_duration_min"),
        F.sum(F.when(F.col("event_type").isin("created", "reopened"), F.col("n_events")).otherwise(0)).alias("n_events_event_type_in_created_reopen"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.sum("duration_min").alias("sum_duration_min_check"),
    )
    .filter(F.col("sum_duration_min_check") > 24.0)
    .drop("sum_duration_min_check")
)

flattened = (
    grouped
    .select(
        "event_type",
        "session_end_day",
        "burst_id",
        "min_n_events",
        "total_n_events",
        "min_duration_min",
        "n_events_event_type_in_created_reopen",
        F.explode_outer("event_type_list").alias("event_type_list_item"),
    )
)

result = (
    flattened
    .select(
        "event_type",
        "session_end_day",
        "burst_id",
        "min_n_events",
        "total_n_events",
        "min_duration_min",
        "n_events_event_type_in_created_reopen",
        "event_type_list_item",
        F.concat(F.col("event_type"), F.lit("#"), F.col("total_n_events").cast("string")).alias("event_type_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type_list_item")), " ", "_")).alias("event_type_list_item_slug"),
    )
    .orderBy(F.col("event_type_list_item").asc())
)
