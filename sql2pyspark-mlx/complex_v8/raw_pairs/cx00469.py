from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_events_moving_1 = Window.orderBy("n_events", "agent_id", "burst_id").rowsBetween(-1, Window.currentRow)
w_session_start_month_agent_id = Window.partitionBy(F.date_trunc("month", "session_start"), "agent_id")
w_all_by_n_events = Window.orderBy("n_events", "agent_id", "burst_id")

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_ref"),
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_agent_id_running))

per_session = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    per_session
    .select(
        "*",
        F.sum("duration_min").over(w_all_by_n_events_moving_1).alias("duration_min_sum_2"),
        F.count(F.lit(1)).over(w_session_start_month_agent_id).alias("rows_in_session_start_month_agent_id"),
        F.max("duration_min").over(w_session_start_month_agent_id).alias("session_start_month_agent_id_max_duration_min"),
        F.round(F.cume_dist().over(w_all_by_n_events), 3).alias("n_events_cume_dist"),
        F.ntile(3).over(w_all_by_n_events).alias("n_events_tercile"),
    )
)

result = (
    sequenced
    .select(
        "agent_id",
        "burst_id",
        "session_end",
        "n_events",
        "duration_min",
        "duration_min_sum_2",
        "rows_in_session_start_month_agent_id",
        "n_events_cume_dist",
        "n_events_tercile",
    )
    .orderBy(F.col("n_events_cume_dist").desc())
)
