from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_to_date_sessio = Window.partitionBy(F.to_date("session_start"), "session_no")
w_n_events_by_n_events = Window.partitionBy("n_events").orderBy(F.col("n_events").desc(), "actor", "session_no")
w_n_events_by_n_events_full = Window.partitionBy("n_events").orderBy(F.col("n_events").desc(), "actor", "session_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    per_session
    .select(
        "*",
        F.round(F.col("duration_min") - F.avg("duration_min").over(w_session_start_to_date_sessio), 2).alias("duration_min_vs_session_start_to_date_session_no"),
        F.round(F.col("duration_min") / F.nullif(F.sum("duration_min").over(w_session_start_to_date_sessio), F.lit(0)), 4).alias("duration_min_share_of_session_start_to_date_sess"),
        F.lag("n_events").over(w_n_events_by_n_events).alias("prev_n_events"),
        F.ntile(5).over(w_n_events_by_n_events).alias("n_events_quintile"),
        F.first("session_no").over(w_n_events_by_n_events_full).alias("first_session_no"),
    )
)

summary = (
    ranked
    .groupBy("first_session_no", "n_events_quintile", "actor")
    .agg(
        F.sum(F.when(F.col("actor") != "system", F.col("prev_n_events")).otherwise(0)).alias("prev_n_events_actor_system"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.round(F.avg("duration_min_vs_session_start_to_date_session_no"), 2).alias("avg_duration_min_vs_session_start_to_date_sessio"),
    )
)

result = (
    summary
    .select(
        "first_session_no",
        "n_events_quintile",
        "actor",
        "distinct_actors",
        F.abs(F.col("n_events_quintile") - 5).alias("n_events_quintile_distance"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
    .orderBy(F.col("first_session_no").asc())
)
