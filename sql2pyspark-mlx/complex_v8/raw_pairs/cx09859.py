from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_closed_at = Window.orderBy(F.col("ticket_closed_at").desc(), "id", "ticket_id_2")
w_ticket_channel = Window.partitionBy("ticket_channel")

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

scored = (
    covered
    .filter(F.col("hours_since_open") > 28.8)
    .select(
        "*",
        F.when(F.col("hours_since_open") >= 43.2, "large").when(F.col("hours_since_open") >= 14.4, "medium").otherwise("small").alias("hours_since_open_tier"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

exploded_detail = (
    scored.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open_tier",
        "s.event_type_or_unknown",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

with_windows = (
    exploded_detail
    .select(
        "*",
        F.lead("ticket_closed_at", 3).over(w_all_by_ticket_closed_at).alias("next3_ticket_closed_at"),
        F.lead("ticket_closed_at").over(w_all_by_ticket_closed_at).alias("next_ticket_closed_at"),
        F.min("ticket_csat").over(w_ticket_channel).alias("ticket_channel_min_ticket_csat"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_all_by_ticket_closed_at))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.max("ticket_csat").over(w_ticket_channel).alias("ticket_channel_max_ticket_csat"),
        F.rank().over(w_all_by_ticket_closed_at).alias("ticket_closed_at_rank"),
    )
)

result = (
    with_windows
    .rollup("event_type", "hours_since_open_tier")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("ticket_csat"), 2).alias("stddev_ticket_csat"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.max("ticket_channel_max_ticket_csat").alias("max_ticket_channel_max_ticket_csat"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("hours_since_open_tier").asc_nulls_last(),
    )
)
