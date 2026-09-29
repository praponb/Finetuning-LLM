from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("hours_since_open").desc(), F.col("id").desc(), F.col("ticket_id_2").desc())

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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

exploded_detail = (
    joined_ranges.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") == "phone"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

latest_per_key = (
    exploded_detail
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ticket_id_stats = (
    latest_per_key
    .groupBy("ticket_id")
    .agg(F.avg("hours_since_open").alias("avg_hours_since_open"))
)

vs_group = (
    latest_per_key.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.hours_since_open") > F.col("g.avg_hours_since_open"))
    .select(
        "s.*",
        "g.avg_hours_since_open",
        F.round(F.col("s.hours_since_open") - F.col("g.avg_hours_since_open"), 2).alias("hours_since_open_minus_avg"),
    )
)

result = (
    vs_group
    .rollup("ticket_id", "ticket_channel")
    .agg(
        F.array_sort(F.collect_set("ticket_priority")).alias("ticket_priority_list"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "ticket_priority_list",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_channel").asc_nulls_last(),
    )
)
