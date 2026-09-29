from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel_by_requeste = Window.partitionBy("agent_id", "channel").orderBy(F.col("requester_id").desc(), "id")
w_closed_at_year_event_count_b = Window.partitionBy(F.year("closed_at"), "event_count").orderBy("requester_id", "id")
w_agent_id_channel_by_requeste_2 = Window.partitionBy("agent_id", "channel").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_tickets = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(6, 10)) & (F.col("agent_id") <= 4))
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
        F.when(F.col("requester_id") >= 20, "A").when(F.col("requester_id") >= 7, "B").otherwise("C").alias("requester_id_tier"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    cleaned_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

screened = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") <= 3) & (~F.array_contains(F.col("t.tags"), "outage")), "left_semi")
)

windowed = (
    screened
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "requester_id_tier",
        "requester_id_bucket",
        F.ntile(4).over(w_agent_id_channel_by_requeste).alias("requester_id_quartile"),
        (F.col("requester_id") - F.lag("requester_id").over(w_closed_at_year_event_count_b)).alias("requester_id_change"),
        F.last("event_count").over(w_agent_id_channel_by_requeste_2).alias("last_event_count"),
    )
)

result = (
    windowed
    .filter(F.col("requester_id_tier") == "B")
    .select(
        "id",
        "priority",
        "requester_id_tier",
        "requester_id_bucket",
        "requester_id_quartile",
        "requester_id_change",
        F.date_trunc("day", F.col("opened_at")).alias("opened_at_day"),
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id_quartile").cast("string")).alias("channel_ref"),
    )
    .orderBy(F.col("requester_id_bucket").asc_nulls_last(), F.col("id").desc())
)
