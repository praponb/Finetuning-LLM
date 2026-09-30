from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_ticket_priority = Window.partitionBy("agent_id", "ticket_priority")
w_all_by_session_end_ties = Window.orderBy("session_end")
w_all_by_session_end = Window.orderBy("session_end", "agent_id", "session_id", "ticket_id")

scoped_tickets = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

with_children = (
    sessions.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority") == "P2"), "inner")
    .select(
        "s.agent_id",
        "s.session_id",
        "s.session_end",
        "s.session_csat",
        "s.duration_min",
        F.col("t.id").alias("ticket_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

ranked = (
    with_children
    .select(
        "*",
        F.sum("duration_min").over(w_agent_id_ticket_priority).alias("agent_id_ticket_priority_sum_duration_min"),
        F.max("session_csat").over(w_agent_id_ticket_priority).alias("agent_id_ticket_priority_max_session_csat"),
        F.rank().over(w_all_by_session_end_ties).alias("session_end_rank"),
        (F.col("session_csat") - F.lag("session_csat").over(w_all_by_session_end)).alias("session_csat_change"),
        F.max("ticket_csat").over(w_agent_id_ticket_priority).alias("agent_id_ticket_priority_max_ticket_csat"),
        F.round(F.col("session_csat") / F.nullif(F.sum("session_csat").over(w_agent_id_ticket_priority), F.lit(0)), 4).alias("session_csat_share_of_agent_id_ticket_priority"),
    )
)

result = (
    ranked
    .groupBy("agent_id", "session_end_rank")
    .pivot("ticket_priority", ["P4", "P1"])
    .agg(F.avg("session_csat_change"))
    .orderBy("agent_id", "session_end_rank")
)
