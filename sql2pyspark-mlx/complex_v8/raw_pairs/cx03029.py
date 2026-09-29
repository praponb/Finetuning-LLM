from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_events = df_ticket_events

with_prev = prepared_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

summary = (
    per_session
    .groupBy(F.year("session_start").alias("session_start_year"), "actor", "burst_id")
    .agg(
        F.sum(F.when(F.col("actor").isin("customer", "agent"), 1).otherwise(0)).alias("n_actor_in_customer_agent"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.max("session_start").alias("last_session_start"),
        F.sum(F.when(F.col("actor").isNotNull(), F.col("duration_min")).otherwise(0)).alias("duration_min_actor_is_not_null"),
    )
)

result = (
    summary
    .filter(F.col("duration_min_actor_is_not_null") > 480.0)
    .select(
        "session_start_year",
        "n_actor_in_customer_agent",
        "distinct_actors",
        "last_session_start",
        "duration_min_actor_is_not_null",
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
    .orderBy(
        F.col("last_session_start").desc(),
        F.col("n_actor_in_customer_agent").desc(),
    )
)
