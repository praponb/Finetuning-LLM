from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id", "priority").orderBy(F.col("last_event_at").desc(), F.col("id").desc())
w_event_count_csat = Window.partitionBy("event_count", "csat")

prepared = (
    df_tickets
    .filter((F.col("csat") > 1) & (F.col("requester_id") < 18))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

latest_per_key = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    latest_per_key
    .select(
        "*",
        F.round(F.col("csat") - F.avg("csat").over(w_event_count_csat), 2).alias("csat_vs_event_count_csat_avg"),
        F.size(F.collect_set("priority").over(w_event_count_csat)).alias("distinct_priority_in_event_count_csat"),
        F.max("csat").over(w_event_count_csat).alias("event_count_csat_max_csat"),
    )
)

result = (
    windowed
    .rollup("channel_or_unknown", "event_count", "channel")
    .agg(
        F.min("distinct_priority_in_event_count_csat").alias("min_distinct_priority_in_event_count_csat"),
    )
    .orderBy(
        F.col("channel_or_unknown").asc_nulls_last(),
        F.col("event_count").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
    )
)
