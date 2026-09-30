from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_by_total_duration_m = Window.partitionBy("burst_id").orderBy("total_duration_min", "agent_id", "burst_id", "session_start_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_total_duration_min = Window.orderBy("total_duration_min", "agent_id", "burst_id", "session_start_month")
w_agent_id_2 = Window.partitionBy("agent_id")

base = (
    df_tickets
    .filter(F.col("agent_id") <= 6)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    sessions
    .filter(F.col("n_events").between(1, 4))
    .groupBy(
        "agent_id",
        "burst_id",
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.max("session_end").alias("last_session_end"),
        F.sum("duration_min").alias("total_duration_min"),
        F.max("duration_min").alias("max_duration_min"),
        F.percentile_approx("duration_min", 0.5).alias("median_duration_min"),
        F.sum("burst_id").alias("total_burst_id"),
    )
)

sequenced = (
    per_group
    .select(
        "*",
        F.last("max_duration_min").over(w_burst_id_by_total_duration_m).alias("last_max_duration_min"),
        F.round((F.unix_timestamp("session_start_month") - F.unix_timestamp(F.lag("session_start_month").over(w_all_by_total_duration_min))) / 60, 1).alias("minutes_since_prev_session_start_month"),
        F.sum("max_duration_min").over(w_agent_id_2).alias("agent_id_sum_max_duration_min"),
    )
)

result = (
    sequenced
    .select(
        "agent_id",
        "burst_id",
        "session_start_month",
        "total_duration_min",
        "max_duration_min",
        "total_burst_id",
        "last_max_duration_min",
        "minutes_since_prev_session_start_month",
        "agent_id_sum_max_duration_min",
        F.round(F.col("max_duration_min") * 1.1, 2).alias("max_duration_min_adj"),
    )
    .orderBy("total_burst_id")
)
