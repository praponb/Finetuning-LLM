from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter(~F.col("event_type").isin("assigned", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = filtered_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_actor_running))

tagged = (
    sessionized
    .select(
        "*",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
    )
)

combined_details = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.channel") != "chat")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.new_session_flag",
        "s.session_id",
        "s.event_at_dayofweek",
        "s.event_at_plus_30m",
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details
    .filter(F.col("new_session_flag").between(0, 1))
    .select(
        "id",
        "actor",
        "new_session_flag",
        "agent_tier",
        "agent_team",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.round(F.col("event_at_dayofweek") / F.nullif(F.col("new_session_flag"), F.lit(0)), 3).alias("event_at_dayofweek_per_new_session_flag"),
    )
    .orderBy(F.col("agent_team").desc(), F.col("actor_len").desc())
)
