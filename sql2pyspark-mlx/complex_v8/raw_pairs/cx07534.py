from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_month = Window.partitionBy(F.month("agent_hired_on"))
w_session_id_session_end_year = Window.partitionBy("session_id", F.year("session_end")).orderBy(F.col("session_end").asc_nulls_last(), "ticket_id", "session_id")
w_session_id_session_end_year_2 = Window.partitionBy("session_id", F.year("session_end")).orderBy(F.col("session_end").asc_nulls_last(), "ticket_id", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_ticket_id_running))

per_session = (
    sessionized
    .groupBy("ticket_id", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched_ticket = (
    per_session.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.channel") == "email")
    .select(
        "s.ticket_id",
        "s.session_id",
        "s.session_start",
        "s.session_end",
        "s.duration_min",
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    enriched_ticket
    .select(
        "*",
        F.round(F.avg("duration_min").over(w_agent_hired_on_month), 2).alias("agent_hired_on_month_avg_duration_min"),
        F.max("session_id").over(w_agent_hired_on_month).alias("agent_hired_on_month_max_session_id"),
        F.row_number().over(w_session_id_session_end_year).alias("session_end_seq"),
        F.nth_value("duration_min", 2).over(w_session_id_session_end_year_2).alias("second_duration_min"),
        F.count(F.lit(1)).over(w_agent_hired_on_month).alias("rows_in_agent_hired_on_month"),
    )
)

result = (
    ranked
    .groupBy("ticket_id", "session_id")
    .pivot("ticket_channel", ["email", "phone"])
    .agg(F.min("duration_min"))
    .orderBy("ticket_id", "session_id")
)
