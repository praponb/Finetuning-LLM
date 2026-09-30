from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_event_at_mont = Window.partitionBy("ticket_channel", F.month("event_at"))
w_event_at_to_date_actor_len_b = Window.partitionBy(F.to_date("event_at"), "actor_len").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_actor_len_b_2 = Window.partitionBy(F.to_date("event_at"), "actor_len").orderBy(F.col("event_at").asc_nulls_last(), "id")

filtered_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

enriched = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.channel") == "email")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_clean",
        "s.actor_len",
        F.col("t.channel").alias("ticket_channel"),
        "su.agent_name",
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("event_type").over(w_ticket_channel_event_at_mont)).alias("distinct_event_type_in_ticket_channel_event_at_m"),
        F.nth_value("actor_len", 2).over(w_event_at_to_date_actor_len_b).alias("second_actor_len"),
        F.rank().over(w_event_at_to_date_actor_len_b_2).alias("event_at_rank"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    with_windows.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

enriched_2 = (
    with_counts
    .select(
        "*",
        F.concat(F.col("ticket_channel"), F.lit("#"), F.col("id").cast("string")).alias("ticket_channel_ref"),
        (F.col("actor_len") - F.col("event_at_rank")).alias("actor_len_minus_event_at_rank"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at_2"),
        F.max("event_at").alias("last_event_at_3"),
        F.count("*").alias("event_count"),
    )
)

summarized_rows = (
    enriched_2.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at_2",
        "t.last_event_at_3",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor_clean",
        "s.actor_len",
        "s.ticket_channel",
        "s.distinct_event_type_in_ticket_channel_event_at_m",
        "s.second_actor_len",
        "s.ticket_channel_ref",
        "s.actor_len_minus_event_at_rank",
        "s.last_event_at_2",
        "s.event_count",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details
    .select(
        "id",
        F.col("second_actor_len").alias("m_second_actor_len"),
        F.col("distinct_event_type_in_ticket_channel_event_at_m").alias("distinct_event_type_in_ticket_channel_event_at_m_2"),
        F.col("event_count").alias("m_event_count"),
        F.col("actor_len_minus_event_at_rank").alias("actor_len_minus_event_at_rank_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_second_actor_len", "distinct_event_type_in_ticket_channel_event_at_m_2", "m_event_count", "actor_len_minus_event_at_rank_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
