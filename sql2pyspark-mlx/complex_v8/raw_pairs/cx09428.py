from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_prev = filtered_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_actor_running))

summary = (
    sessionized
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"), "ticket_id")
    .agg(
        F.sum("starts_session").alias("total_starts_session"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.sum(F.when(~F.col("event_type").isin("created", "resolved"), 1).otherwise(0)).alias("n_event_type_not_in_created_re"),
        F.count("*").alias("row_count"),
        F.avg("session_no").alias("avg_session_no_check"),
    )
    .filter(F.col("avg_session_no_check") > 2)
    .drop("avg_session_no_check")
)

qualified = (
    summary.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("assigned", "reopened")), "left_semi")
)

result = (
    qualified
    .filter(F.col("n_event_type_not_in_created_re").between(3, 8))
    .select("event_at_month", "ticket_id", "distinct_actors")
    .orderBy(F.col("event_at_month").desc(), F.col("distinct_actors").desc())
)
