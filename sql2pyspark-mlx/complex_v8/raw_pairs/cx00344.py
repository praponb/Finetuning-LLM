from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id_by_requeste = Window.partitionBy("channel", "agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_closed_at_plus_7d = Window.orderBy("closed_at_plus_7d", "id")

valid_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_id").cast("string")).alias("channel_ref"),
        (F.col("closed_at") + F.expr("INTERVAL 7 DAYS")).alias("closed_at_plus_7d"),
    )
)

annotated = (
    valid_tickets
    .select(
        "*",
        F.date_trunc("quarter", F.col("opened_at")).alias("opened_at_quarter"),
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_ref_2"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

qualified = (
    annotated.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")), "left_anti")
)

windowed = (
    qualified
    .select(
        "id",
        "opened_at",
        "channel",
        "csat",
        "channel_ref",
        "closed_at_plus_7d",
        "opened_at_quarter",
        F.sum("csat").over(w_channel_agent_id_by_requeste).alias("csat_sum_3_prior"),
        F.row_number().over(w_all_by_closed_at_plus_7d).alias("seq_no"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor") != "system", 1)).alias("event_actor_count"),
    )
)

profiled = (
    windowed.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "opened_at",
        "channel",
        "csat",
        "channel_ref",
        "csat_sum_3_prior",
        "seq_no",
        "last_event_at",
        "event_actor_count",
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
        F.round((F.unix_timestamp(F.col("closed_at_plus_7d")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at_plus_7d"),
    )
    .orderBy(F.col("csat").desc(), "csat_sum_3_prior", "id")
    .limit(5)
)
