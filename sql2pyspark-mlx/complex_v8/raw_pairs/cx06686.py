from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("agent_id") <= 4)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        F.size(F.col("tags")).alias("tags_count"),
    )
)

with_prev = filtered_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("tags_count").alias("session_tags_count"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .groupingSets(
        [["n_events", "session_tags_count", "session_id"], ["n_events"], []],
        "n_events", "session_tags_count", "session_id",
    )
    .agg(F.max("session_start").alias("last_session_start"))
    .orderBy(
        F.col("n_events").asc_nulls_last(),
        F.col("session_tags_count").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
    )
)
