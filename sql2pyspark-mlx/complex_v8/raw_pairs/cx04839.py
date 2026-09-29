from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_new_session_flag_pr = Window.partitionBy("actor").orderBy("new_session_flag", "id").rowsBetween(-2, -1)
w_new_session_flag_session_id = Window.partitionBy("new_session_flag", "session_id").orderBy("event_at", "id")
w_latest = Window.partitionBy("event_type").orderBy(F.col("session_id").desc(), F.col("id").desc())

scoped = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_actor_running))

metrics = (
    sessionized
    .select(
        "*",
        F.max("session_id").over(w_actor_by_new_session_flag_pr).alias("session_id_max_2_prior"),
        F.row_number().over(w_new_session_flag_session_id).alias("event_at_seq"),
    )
)

latest_per_key = (
    metrics
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "prev_event_at",
        "new_session_flag",
        "session_id_max_2_prior",
    )
)
