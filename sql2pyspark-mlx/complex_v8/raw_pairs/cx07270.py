from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_opened_at_plus_30m_month_age = Window.partitionBy(F.month("opened_at_plus_30m"), "agent_id").orderBy(F.col("opened_at").desc(), "id")
w_channel_agent_id_by_first_ev = Window.partitionBy("channel", "agent_id").orderBy("first_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_agent_id_by_first_ev_2 = Window.partitionBy("channel", "agent_id").orderBy("first_event_at")
w_opened_at_plus_30m_year = Window.partitionBy(F.year("opened_at_plus_30m"))
w_channel_agent_id_by_first_ev_3 = Window.partitionBy("channel", "agent_id").orderBy("first_event_at", "id")

filtered = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-01-28 19:00:00").cast("timestamp")) & (F.array_contains(F.col("tags"), "outage")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.lead("closed_at").over(w_all_by_opened_at).alias("next_closed_at"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_opened_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.percent_rank().over(w_opened_at_plus_30m_month_age), 3).alias("opened_at_percent_rank"),
        F.dense_rank().over(w_opened_at_plus_30m_month_age).alias("opened_at_dense_rank"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

summarized_rows = (
    metrics.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at", "t.first_event_at_2")
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.last("minutes_since_prev_closed_at", ignorenulls=True).over(w_channel_agent_id_by_first_ev).alias("last_known_minutes_since_prev_closed_at"),
        F.dense_rank().over(w_channel_agent_id_by_first_ev_2).alias("first_event_at_dense_rank"),
        F.round(F.avg("minutes_since_prev_closed_at").over(w_opened_at_plus_30m_year), 2).alias("opened_at_plus_30m_year_avg_minutes_since_prev_c"),
        F.ntile(3).over(w_channel_agent_id_by_first_ev_3).alias("first_event_at_tercile"),
    )
)

detail_rows_events = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isNotNull()), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.opened_at_plus_30m",
        "s.channel_priority_label",
        "s.minutes_since_prev_closed_at",
        "s.opened_at_dense_rank",
        "s.first_event_at",
        "s.first_event_at_2",
        "s.last_known_minutes_since_prev_closed_at",
        "s.opened_at_plus_30m_year_avg_minutes_since_prev_c",
        "s.first_event_at_tercile",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

result = (
    detail_rows_events
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "channel_priority_label",
        "minutes_since_prev_closed_at",
        "opened_at_dense_rank",
        "first_event_at",
        "last_known_minutes_since_prev_closed_at",
        "first_event_at_tercile",
        "event_id",
        "event_at",
        "event_type",
        (F.floor(F.col("first_event_at_tercile") / 2) * 2).alias("first_event_at_tercile_bucket"),
        F.date_trunc("quarter", F.col("opened_at_plus_30m")).alias("opened_at_plus_30m_quarter"),
    )
    .orderBy(F.col("channel").desc())
)
