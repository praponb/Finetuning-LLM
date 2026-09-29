from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("priority", "event_type").orderBy(F.col("requester_id").desc(), F.col("id").desc(), F.col("event_id").desc())

base_tickets = (
    df_tickets
    .filter((F.col("channel").isin("email", "web")) & (F.col("csat") >= 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

line_level_events = (
    base_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.channel_or_unknown",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

filtered = (
    line_level_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.id") == F.col("s.event_id")) & (F.hour(F.col("t.event_at")).between(8, 13)) & (F.col("t.event_type") != "replied"), "left_anti")
)

latest_per_key = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "event_id",
        "event_actor",
        F.initcap(F.trim(F.col("event_actor"))).alias("event_actor_clean"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
    .orderBy(F.col("event_actor_clean").desc())
)
