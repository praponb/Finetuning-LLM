from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_ticket_requester = Window.orderBy("avg_ticket_requester_id", "id")

valid = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        F.date_trunc("week", F.col("closed_at")).alias("closed_at_week"),
        F.year(F.col("opened_at")).alias("opened_at_year"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.round(F.avg("requester_id"), 2).alias("avg_ticket_requester_id"))
)

with_stats = (
    valid.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.avg_ticket_requester_id")
)

with_windows = (
    with_stats
    .select(
        "*",
        F.ntile(5).over(w_all_by_avg_ticket_requester).alias("avg_ticket_requester_id_quintile"),
        F.rank().over(w_all_by_avg_ticket_requester).alias("avg_ticket_requester_id_rank"),
    )
)

line_level = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.opened_at_year",
        "s.avg_ticket_requester_id_rank",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

with_details = (
    line_level.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.opened_at_year",
        "s.avg_ticket_requester_id_rank",
        "s.event_id",
        "s.event_actor",
        F.col("t.actor").alias("event_actor_2"),
    )
)

enriched = (
    with_details
    .select(
        "id",
        "agent_id",
        "channel",
        "opened_at_year",
        "avg_ticket_requester_id_rank",
        "event_id",
        "event_actor",
        "event_actor_2",
        F.coalesce(F.col("opened_at_year"), F.lit(0)).alias("opened_at_year_filled"),
        F.coalesce(F.col("opened_at_year"), F.lit(0)).alias("opened_at_year_filled_2"),
        F.coalesce(F.col("event_actor_2"), F.lit("UNKNOWN")).alias("event_actor_2_or_unknown"),
    )
)

totals = (
    enriched
    .groupBy("event_actor_2", "avg_ticket_requester_id_rank", "event_id")
    .agg(
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("n_channel_web"),
        F.count("*").alias("row_count"),
        F.sum(F.when(~F.col("channel").isin("web", "email"), 1).otherwise(0)).alias("n_channel_not_in_web_email"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    totals
    .select(
        "event_actor_2",
        "avg_ticket_requester_id_rank",
        "event_id",
        "n_channel_web",
        "row_count",
        "n_channel_not_in_web_email",
        (F.col("n_channel_web") + F.col("row_count")).alias("n_channel_web_plus_row_count"),
    )
    .orderBy("row_count", F.col("event_actor_2").asc())
)
