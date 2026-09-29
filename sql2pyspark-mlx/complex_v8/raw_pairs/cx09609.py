from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = valid.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_ticket_id_running))

joined = (
    sessionized.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.channel") != "chat")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.new_session_flag",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    joined
    .cube("new_session_flag", "event_type", "ticket_id")
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.min("new_session_flag").alias("min_new_session_flag"),
    )
)
