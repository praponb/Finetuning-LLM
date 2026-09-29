from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = base_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_actor_running))

scored = sessionized.withColumn("prev_event_at_month", F.date_trunc("month", F.col("prev_event_at")))

labeled = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "prev_event_at",
        "session_no",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

result = (
    labeled
    .groupingSets(
        [["session_no", "ticket_id"], ["session_no"], []],
        "session_no", "ticket_id",
    )
    .agg(
        F.grouping("session_no").alias("is_total_session_no"),
        F.round(100 * F.count(F.when(F.col("actor").isin("system", "agent"), 1)) / F.count("*"), 1).alias("pct_actor_in_system_agent"),
    )
)
