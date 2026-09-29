from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = df_ticket_events

with_prev = valid.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_actor_running))

session_summary = (
    sessionized
    .groupBy("actor", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    session_summary
    .select(
        "*",
        (F.col("session_start") + F.expr("INTERVAL 1 DAYS")).alias("session_start_plus_1d"),
        F.concat(F.col("actor"), F.lit("#"), F.col("n_events").cast("string")).alias("actor_ref"),
    )
)

result = (
    scored
    .select(
        "actor",
        "session_no",
        "session_end",
        "n_events",
        "duration_min",
        "session_start_plus_1d",
        "actor_ref",
        F.round(F.col("duration_min") / F.nullif(F.col("session_no"), F.lit(0)), 3).alias("duration_min_per_session_no"),
    )
    .orderBy(F.col("duration_min").desc(), F.col("session_no").asc_nulls_last())
)
