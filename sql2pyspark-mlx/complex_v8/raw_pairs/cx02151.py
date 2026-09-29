from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id", "priority").orderBy(F.col("closed_at").desc(), F.col("id").desc())

valid = (
    df_tickets
    .filter(F.col("priority") == "P4")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.concat_ws("|", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

unique_rows = (
    valid
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

summarized_rows = (
    unique_rows.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.first_event_at",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel_priority_label",
        "last_event_at",
        "event_actor_count",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
    .orderBy(F.col("requester_id").asc_nulls_last())
)
