from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end_running = Window.orderBy("session_end", "actor", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_start_ties = Window.orderBy("session_start")
w_all_by_session_start_moving = Window.orderBy("session_start", "actor", "session_id").rowsBetween(-1, Window.currentRow)
w_all_by_session_start_moving_2 = Window.orderBy("session_start", "actor", "session_id").rowsBetween(-6, Window.currentRow)
w_session_id_actor = Window.partitionBy("session_id", "actor")
w_all_by_session_start_running = Window.orderBy("session_start", "actor", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date"))
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    per_session
    .select(
        "*",
        F.min("session_id").over(w_all_by_session_end_running).alias("min_session_id_so_far"),
        F.round(F.percent_rank().over(w_all_by_session_start_ties), 3).alias("session_start_percent_rank"),
        F.round(F.avg("duration_min").over(w_all_by_session_start_moving), 2).alias("duration_min_avg_2"),
        F.round(F.avg("duration_min").over(w_all_by_session_start_moving_2), 2).alias("duration_min_avg_7"),
        F.round(F.avg("session_id").over(w_session_id_actor), 2).alias("session_id_actor_avg_session_id"),
        F.last("session_start", ignorenulls=True).over(w_all_by_session_start_running).alias("last_known_session_start"),
    )
)

result = (
    sequenced
    .filter(F.col("session_id_actor_avg_session_id") <= 5.2)
    .select(
        "actor",
        "session_id",
        "session_start",
        "events",
        "min_session_id_so_far",
        "session_start_percent_rank",
        "duration_min_avg_2",
        "session_id_actor_avg_session_id",
        "last_known_session_start",
        F.concat(F.col("actor"), F.lit("#"), F.col("events").cast("string")).alias("actor_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
    .orderBy(F.col("session_id").desc())
)
