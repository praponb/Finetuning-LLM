from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_session_end = Window.partitionBy("actor").orderBy(F.col("session_end").desc(), "actor", "session_no")
w_all_by_n_events_running = Window.orderBy(F.col("n_events").desc(), "actor", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_events = Window.orderBy(F.col("n_events").desc(), "actor", "session_no")
w_session_end_month = Window.partitionBy(F.month("session_end"))

filtered = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_actor_running))

sessions = (
    sessionized
    .groupBy("actor", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    sessions
    .select(
        "*",
        F.lead("n_events").over(w_actor_by_session_end).alias("next_n_events"),
        F.min("session_no").over(w_all_by_n_events_running).alias("min_session_no_so_far"),
        F.round((F.unix_timestamp("session_start") - F.unix_timestamp(F.lag("session_start").over(w_all_by_n_events))) / 60, 1).alias("minutes_since_prev_session_start"),
        F.min("n_events").over(w_session_end_month).alias("session_end_month_min_n_events"),
        F.lead("session_no", 1, 0).over(w_all_by_n_events).alias("next_session_no"),
    )
)

result = (
    metrics
    .select(
        "actor",
        "session_no",
        "session_end",
        "duration_min",
        "next_n_events",
        "min_session_no_so_far",
        "next_session_no",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
    .orderBy(
        F.col("actor_clean").desc_nulls_first(),
        F.col("min_session_no_so_far").desc(),
    )
)
