from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = df_tickets.filter(F.col("csat") <= 4)

with_prev = prepared.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent_agent = (
    session_summary.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.session_no",
        "s.session_start",
        "s.session_end",
        "s.event_count",
        "s.session_csat",
        "s.duration_min",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

enriched = (
    with_parent_agent
    .select(
        "*",
        F.substring(F.trim(F.col("agent_tier")), 1, 3).alias("agent_tier_prefix"),
        F.concat_ws(" / ", F.col("agent_name"), F.col("agent_tier")).alias("agent_name_agent_tier_label"),
        F.round(F.col("session_no") / F.nullif(F.col("session_csat"), F.lit(0)), 3).alias("session_no_per_session_csat"),
    )
)

result = (
    enriched
    .rollup("event_count", "agent_id", "agent_tier")
    .agg(
        F.round(F.sum("duration_min") / F.nullif(F.sum("session_csat"), F.lit(0)), 3).alias("duration_min_to_session_csat_ratio"),
        F.round(F.stddev("event_count"), 2).alias("stddev_event_count"),
    )
    .select(
        "event_count",
        "agent_id",
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "duration_min_to_session_csat_ratio",
        "stddev_event_count",
    )
    .orderBy(
        F.col("event_count").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
