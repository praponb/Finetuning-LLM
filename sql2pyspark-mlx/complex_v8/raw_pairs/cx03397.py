from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = recent_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_actor_running))

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

stats = (
    session_summary
    .groupBy(
        F.date_trunc("month", "session_end").alias("session_end_month"),
        "n_events",
    )
    .agg(
        F.round(F.sum("n_events") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("n_events_to_session_no_ratio"),
        F.sum("session_no").alias("total_session_no"),
        F.count("*").alias("row_count"),
        F.round(F.sum("session_no") / F.nullif(F.sum("duration_min"), F.lit(0)), 3).alias("session_no_to_duration_min_ratio"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.count(F.when(F.col("actor").isin("customer", "system"), 1)).alias("n_actor_in_customer_system"),
    )
)

result = (
    stats
    .select(
        "session_end_month",
        "n_events",
        "n_actor_in_customer_system",
        "row_count",
        "distinct_actors",
    )
    .unpivot(
        ids=["session_end_month", "n_events"],
        values=["n_actor_in_customer_system", "row_count", "distinct_actors"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
