from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_session_start_ties = Window.partitionBy("actor").orderBy(F.col("session_start").desc_nulls_first())
w_actor_by_session_start = Window.partitionBy("actor").orderBy(F.col("session_start").desc_nulls_first(), "actor", "session_id")
w_all_by_session_end_ties = Window.orderBy("session_end")

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = recent.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    per_session
    .crossJoin(per_session.agg(F.min("duration_min").alias("overall_min_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_min_duration_min"))
    .drop("overall_min_duration_min")
)

ranked = (
    without_matches
    .select(
        "*",
        F.rank().over(w_actor_by_session_start_ties).alias("session_start_rank"),
        F.row_number().over(w_actor_by_session_start).alias("seq_no"),
        F.lead("duration_min", 2, 0).over(w_actor_by_session_start).alias("next2_duration_min"),
        F.dense_rank().over(w_all_by_session_end_ties).alias("session_end_dense_rank"),
    )
)

result = (
    ranked
    .filter(F.col("session_end_dense_rank") <= 8)
    .select(
        "actor",
        "session_id",
        "session_start",
        "n_events",
        "duration_min",
        "session_start_rank",
        "seq_no",
        "session_end_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
    .orderBy(F.col("session_start").desc_nulls_first())
)
