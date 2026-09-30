from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_latest = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at_percent_rank").desc(), F.col("id").desc())

recent_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

latest_per_key = (
    recent_events
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest_per_key
    .select(
        "id",
        "ticket_id",
        "event_type",
        F.ntile(10).over(w_actor_event_type_by_event_at).alias("event_at_decile"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

combined_details = (
    metrics.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_at_decile",
        "s.event_at_percent_rank",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

qualified = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()) & (F.col("t.event_type").isNotNull()), "left_semi")
)

latest = (
    qualified
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .filter(~F.col("agent_tier").isin("T3", "T2"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_at_decile",
        "event_at_percent_rank",
        "ticket_tags",
        "ticket_channel",
        "agent_team",
        "agent_tier",
    )
    .orderBy(F.col("event_type").desc(), F.col("agent_tier").desc(), "id")
    .limit(5)
)
