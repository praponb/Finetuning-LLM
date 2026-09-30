from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_year = Window.partitionBy(F.year("session_start"))
w_burst_id_event_count_by_even = Window.partitionBy("burst_id", "event_count").orderBy(F.col("event_count").asc_nulls_last(), "agent_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_agent_id_by_event_c = Window.partitionBy("burst_id", "agent_id").orderBy("event_count", "agent_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_burst_id_agent_id_by_event_c_2 = Window.partitionBy("burst_id", "agent_id").orderBy("event_count", "agent_id", "burst_id")
w_burst_id_event_count_by_even_2 = Window.partitionBy("burst_id", "event_count").orderBy(F.col("event_count").asc_nulls_last())

cleaned = (
    df_tickets
    .filter(F.col("csat").between(1, 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = cleaned.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    sessions
    .select(
        "*",
        F.round(F.col("duration_min") - F.avg("duration_min").over(w_session_start_year), 2).alias("duration_min_vs_session_start_year_avg"),
        F.last("session_start", ignorenulls=True).over(w_burst_id_event_count_by_even).alias("last_known_session_start"),
        F.nth_value("event_count", 2).over(w_burst_id_agent_id_by_event_c).alias("second_event_count"),
        F.dense_rank().over(w_burst_id_agent_id_by_event_c_2).alias("event_count_dense_rank"),
        F.dense_rank().over(w_burst_id_event_count_by_even_2).alias("event_count_dense_rank_2"),
        F.size(F.collect_set("agent_id").over(w_session_start_year)).alias("distinct_agent_id_in_session_start_year"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

profiled = (
    sequenced.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    profiled
    .select(
        "agent_id",
        "session_end",
        "event_count",
        "session_csat",
        "duration_min_vs_session_start_year_avg",
        "last_known_session_start",
        "event_count_dense_rank_2",
        "distinct_agent_id_in_session_start_year",
        (F.col("session_start") + F.expr("INTERVAL 1 DAYS")).alias("session_start_plus_1d"),
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 3600, 1).alias("hours_session_end_to_session_start"),
    )
    .orderBy(F.col("session_start_plus_1d").desc(), "event_count_dense_rank_2")
)
