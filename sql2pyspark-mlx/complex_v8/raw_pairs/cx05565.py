from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

with_prev = scoped.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

rollup_base = (
    sessions
    .groupBy(
        F.date_trunc("month", "session_end").alias("session_end_month"),
        "n_events",
    )
    .agg(
        F.round(F.sum("duration_min"), 2).alias("total_duration_min"),
        F.percentile_approx("burst_id", 0.5).alias("median_burst_id"),
        F.max("session_start").alias("last_session_start"),
        F.sum("n_events").alias("total_n_events"),
        F.percentile_approx("duration_min", 0.5).alias("median_duration_min"),
    )
)

result = (
    rollup_base
    .select(
        "session_end_month",
        "n_events",
        "median_burst_id",
        "last_session_start",
        "total_n_events",
        "median_duration_min",
        F.when(F.col("median_burst_id") >= 5, "HIGH").when(F.col("median_burst_id") >= 3, "MEDIUM").otherwise("LOW").alias("median_burst_id_tier"),
    )
    .orderBy(
        F.col("session_end_month").asc_nulls_last(),
        F.col("total_n_events").asc(),
    )
)
