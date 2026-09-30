from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = df_tickets

with_prev = scoped.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

with_children = (
    session_summary.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") != "email"), "left")
    .select(
        "s.agent_id",
        "s.burst_id",
        "s.session_start",
        "s.session_end",
        "s.events",
        "s.session_requester_id",
        "s.duration_min",
        F.col("t.id").alias("ticket_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

filtered = (
    with_children.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") == "web"), "left_anti")
)

result = (
    filtered
    .rollup("ticket_id", "agent_id")
    .agg(
        F.grouping("ticket_id").alias("is_total_ticket_id"),
        F.round(F.sum("duration_min"), 2).alias("total_duration_min"),
        F.round(F.sum("duration_min") / F.nullif(F.sum("burst_id"), F.lit(0)), 3).alias("duration_min_to_burst_id_ratio"),
    )
    .orderBy(F.col("ticket_id").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
