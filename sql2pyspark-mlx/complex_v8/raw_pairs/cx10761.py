from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter(F.col("priority").isin("P3", "P4"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .select(
        "agent_id",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
    )
    .orderBy(F.col("event_count").desc(), F.col("session_no").asc(), "agent_id")
    .limit(5)
)
