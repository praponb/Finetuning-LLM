from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no_actor_by_session = Window.partitionBy("session_no", "actor").orderBy("session_end", "actor", "session_no")
w_session_no_actor_by_session_2 = Window.partitionBy("session_no", "actor").orderBy("session_end")
w_session_no_actor_by_session_3 = Window.partitionBy("session_no", "actor").orderBy("session_end", "actor", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
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

with_windows = (
    sessions
    .select(
        "*",
        F.ntile(3).over(w_session_no_actor_by_session).alias("session_end_tercile"),
        F.lag("session_start", 2).over(w_session_no_actor_by_session).alias("prev2_session_start"),
        F.rank().over(w_session_no_actor_by_session_2).alias("session_end_rank"),
        F.first("session_start", ignorenulls=True).over(w_session_no_actor_by_session_3).alias("first_known_session_start"),
    )
)

result = (
    with_windows
    .filter(F.col("session_end_rank").between(3, 8))
    .select(
        "actor",
        "session_no",
        "session_start",
        "duration_min",
        "session_end_tercile",
        "prev2_session_start",
        "session_end_rank",
        "first_known_session_start",
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
    .orderBy(
        "duration_min_filled",
        F.col("first_known_session_start").asc_nulls_last(),
    )
)
