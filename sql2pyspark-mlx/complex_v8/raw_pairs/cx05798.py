from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_to_date_channe = Window.partitionBy(F.to_date("session_start"), "channel")
w_all_by_session_csat_ties = Window.orderBy("session_csat")

base_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
    )
)

with_prev = base_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_channel_running))

per_session = (
    sessionized
    .groupBy("channel", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    per_session
    .select(
        "channel",
        "session_id",
        "session_start",
        "n_events",
        "session_csat",
        "duration_min",
        F.count(F.lit(1)).over(w_session_start_to_date_channe).alias("rows_in_session_start_to_date_channel"),
        F.round(F.col("duration_min") / F.nullif(F.sum("duration_min").over(w_session_start_to_date_channe), F.lit(0)), 4).alias("duration_min_share_of_session_start_to_date_chan"),
        F.round(F.col("n_events") / F.nullif(F.sum("n_events").over(w_session_start_to_date_channe), F.lit(0)), 4).alias("n_events_share_of_session_start_to_date_channel"),
        F.sum("session_id").over(w_session_start_to_date_channe).alias("session_start_to_date_channel_sum_session_id"),
        F.round(F.cume_dist().over(w_all_by_session_csat_ties), 3).alias("session_csat_cume_dist"),
    )
)

enriched = (
    with_trends
    .filter(F.col("session_csat") > 3)
    .select(
        "*",
        F.when(F.col("channel").isin("phone", "email"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

result = (
    enriched
    .select(
        "session_start_to_date_channel_sum_session_id",
        "session_id",
        "duration_min",
    )
    .distinct()
    .orderBy(F.col("duration_min").desc_nulls_first())
)
