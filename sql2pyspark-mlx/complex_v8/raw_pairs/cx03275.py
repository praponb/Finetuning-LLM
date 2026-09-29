from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_greatest_requeste = Window.partitionBy("channel").orderBy(F.col("greatest_requester_id_event_count").desc())
w_channel_by_greatest_requeste_2 = Window.partitionBy("channel").orderBy(F.col("greatest_requester_id_event_count").desc(), "id")
w_priority_last_event_at_year = Window.partitionBy("priority", F.year("last_event_at"))
w_opened_at_year_month_channel = Window.partitionBy(F.date_trunc("month", "opened_at_year"), "channel").orderBy("csat_minus_event_count", "id").rowsBetween(-2, -1)

filtered = (
    df_tickets
    .filter(F.col("priority").isin("P2", "P3", "P4"))
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
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

scored = (
    with_stats
    .filter(F.col("priority") == "P4")
    .select(
        "*",
        (F.col("csat") - F.col("event_count")).alias("csat_minus_event_count"),
        F.greatest(F.col("requester_id"), F.col("event_count")).alias("greatest_requester_id_event_count"),
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "opened_at_year",
        "event_count",
        "csat_minus_event_count",
        "greatest_requester_id_event_count",
        F.round(F.percent_rank().over(w_channel_by_greatest_requeste), 3).alias("greatest_requester_id_event_count_percent_rank"),
        F.round((F.unix_timestamp("last_event_at") - F.unix_timestamp(F.lag("last_event_at").over(w_channel_by_greatest_requeste_2))) / 60, 1).alias("minutes_since_prev_last_event_at"),
        F.round(F.col("event_count") / F.nullif(F.sum("event_count").over(w_priority_last_event_at_year), F.lit(0)), 4).alias("event_count_share_of_priority_last_event_at_year"),
        F.round(F.avg("csat_minus_event_count").over(w_opened_at_year_month_channel), 2).alias("csat_minus_event_count_avg_2_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "opened_at",
        "channel",
        "opened_at_year",
        "csat_minus_event_count",
        "greatest_requester_id_event_count",
        "greatest_requester_id_event_count_percent_rank",
        "minutes_since_prev_last_event_at",
        "event_count_share_of_priority_last_event_at_year",
        "csat_minus_event_count_avg_2_prior",
        F.round(F.col("event_count") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("event_count_per_csat"),
        F.when(F.col("channel").isin("web", "chat"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
    .orderBy(
        F.col("channel_group").desc(),
        F.col("csat_minus_event_count_avg_2_prior").desc(),
    )
)
