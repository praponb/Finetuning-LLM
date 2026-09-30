from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_actor_running))

result = (
    sessionized
    .groupingSets(
        [["ticket_id", "actor", "session_id"], ["ticket_id"], ["actor"]],
        "ticket_id", "actor", "session_id",
    )
    .agg(F.min("prev_event_at").alias("first_prev_event_at"))
    .select(
        "ticket_id",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "session_id",
        "first_prev_event_at",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
    )
)
