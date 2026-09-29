from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .groupingSets(
        [["agent_id", "session_id", "events"], ["agent_id"], []],
        "agent_id", "session_id", "events",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("session_end").alias("last_session_end"),
        F.min("session_end").alias("first_session_end"),
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
        F.col("events").asc_nulls_last(),
    )
)
