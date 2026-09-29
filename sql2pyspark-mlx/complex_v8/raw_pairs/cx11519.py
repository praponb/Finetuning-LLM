from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .filter(F.col("event_type") != "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_prev = recent_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_actor_running))

actor_stats = (
    sessionized
    .groupBy("actor")
    .agg(F.max("new_session_flag").alias("max_new_session_flag"))
)

compared = (
    sessionized.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.new_session_flag") == F.col("g.max_new_session_flag"))
    .select(
        "s.*",
        "g.max_new_session_flag",
        F.round(F.col("s.new_session_flag") - F.col("g.max_new_session_flag"), 2).alias("new_session_flag_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_or_unknown",
        "prev_event_at",
        "new_session_flag",
        "session_no",
        "max_new_session_flag",
        F.round(F.col("max_new_session_flag") * 1.1, 2).alias("max_new_session_flag_adj"),
    )
    .orderBy(F.col("actor").desc(), "id")
    .limit(20)
)
