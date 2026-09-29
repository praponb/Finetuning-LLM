from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id = Window.partitionBy("session_id")
w_session_id_max_session_id_by = Window.partitionBy("session_id", "max_session_id").orderBy(F.col("total_session_id").asc_nulls_last(), "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = df_tickets.filter(F.col("requester_id") < 20)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_agent_id_running))

per_session = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    per_session
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("event_count"), 2).alias("stddev_event_count"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.min("duration_min").alias("min_duration_min"),
        F.sum("session_id").alias("total_session_id"),
        F.max("session_id").alias("max_session_id"),
    )
)

sequenced = (
    stats
    .select(
        "session_id",
        "distinct_agent_ids",
        "total_session_id",
        "max_session_id",
        F.count(F.lit(1)).over(w_session_id).alias("rows_in_session_id"),
        F.first("distinct_agent_ids", ignorenulls=True).over(w_session_id_max_session_id_by).alias("first_known_distinct_agent_ids"),
    )
)

result = (
    sequenced
    .filter(F.col("distinct_agent_ids") < 9)
    .select(
        "session_id",
        "distinct_agent_ids",
        "max_session_id",
        "first_known_distinct_agent_ids",
        (F.col("distinct_agent_ids") - F.col("max_session_id")).alias("distinct_agent_ids_minus_max_session_id"),
    )
    .orderBy("distinct_agent_ids_minus_max_session_id")
)
