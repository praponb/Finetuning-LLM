from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id", "channel").orderBy(F.col("csat").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("agent_id", "agent_team").orderBy("opened_at", "id", "event_id")

filtered = (
    df_tickets
    .filter(F.col("channel") == "email")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix_2"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

combined_details_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

qualified = (
    combined_details_agent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "customer"), "left_anti")
)

unique_rows = (
    qualified
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

exploded_detail_events = (
    unique_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.priority_prefix_2",
        "s.agent_team",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    exploded_detail_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

deduped = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "agent_team",
        "event_id",
        "event_type",
        "event_actor",
        "last_event_at",
        F.lower(F.regexp_replace(F.trim(F.col("priority_prefix_2")), " ", "_")).alias("priority_prefix_2_slug"),
        F.concat(F.col("agent_team"), F.lit("#"), F.col("id").cast("string")).alias("agent_team_ref"),
    )
    .orderBy(
        F.col("event_actor").desc(),
        "opened_at",
        "id",
        "event_id",
    )
    .limit(5)
)
