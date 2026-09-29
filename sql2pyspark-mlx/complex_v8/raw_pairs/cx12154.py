from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.date_format(F.col("closed_at"), "MMM yyyy").alias("closed_at_label"),
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    session_summary
    .select(
        "*",
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
        (F.col("session_id") - F.col("duration_min")).alias("session_id_minus_duration_min"),
    )
)

result = (
    enriched
    .select(
        "agent_id",
        "session_end",
        "event_count",
        "duration_min_adj",
        "session_id_minus_duration_min",
        F.year(F.col("session_start")).alias("session_start_year"),
    )
    .orderBy(F.col("event_count").desc())
)
