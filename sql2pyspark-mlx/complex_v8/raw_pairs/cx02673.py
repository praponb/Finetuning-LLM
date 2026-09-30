from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_priority_running))

session_summary = (
    sessionized
    .groupBy("priority", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    session_summary
    .filter(F.col("priority").isin("P1", "P3", "P2"))
    .select(
        "priority",
        "burst_id",
        "session_start",
        "n_events",
        "session_csat",
        "duration_min",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.date_format(F.col("session_start"), "MMM yyyy").alias("session_start_label"),
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
    )
)

enriched = (
    derived
    .select(
        "*",
        F.round(F.col("session_csat") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("session_csat_per_duration_min"),
    )
)

result = (
    enriched
    .select(
        "priority",
        "burst_id",
        "session_start",
        "duration_min",
        "priority_slug",
        "session_start_label",
        "priority_prefix",
        "session_csat_per_duration_min",
        (F.floor(F.col("session_csat") / 5) * 5).alias("session_csat_bucket"),
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
    )
    .orderBy(F.col("session_start").desc(), "priority")
)
