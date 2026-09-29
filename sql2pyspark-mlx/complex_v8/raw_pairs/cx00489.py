from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_filled_csat_by_csat_fil = Window.partitionBy("csat_filled", "csat").orderBy(F.col("csat_filled").desc(), "id")
w_channel_agent_id_by_csat_tie = Window.partitionBy("channel", "agent_id").orderBy("csat")
w_csat_filled_csat_by_csat_fil_2 = Window.partitionBy("csat_filled", "csat").orderBy(F.col("csat_filled").desc())
w_opened_at_to_date = Window.partitionBy(F.to_date("opened_at"))
w_channel_agent_id_by_csat = Window.partitionBy("channel", "agent_id").orderBy("csat", "id")

filtered_tickets = (
    df_tickets
    .filter(F.col("channel").isNotNull())
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
        F.when(F.col("priority").isin("P2", "P3"), "group_a").when(F.col("priority") == "P4", "group_b").otherwise("other").alias("priority_group"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

ranked = (
    filtered_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "priority_group",
        "csat_filled",
        F.lead("opened_at").over(w_csat_filled_csat_by_csat_fil).alias("next_opened_at"),
        F.dense_rank().over(w_channel_agent_id_by_csat_tie).alias("csat_dense_rank"),
        F.round(F.cume_dist().over(w_csat_filled_csat_by_csat_fil_2), 3).alias("csat_filled_cume_dist"),
        F.count(F.lit(1)).over(w_opened_at_to_date).alias("rows_in_opened_at_to_date"),
        (F.col("csat") - F.lag("csat").over(w_channel_agent_id_by_csat)).alias("csat_change"),
        F.lag("closed_at_hour", 3).over(w_channel_agent_id_by_csat).alias("prev3_closed_at_hour"),
    )
)

qualified = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
    )
)

summarized_rows = (
    qualified.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

channel_stats = (
    summarized_rows
    .groupBy("channel")
    .agg(F.max("csat_filled").alias("max_csat_filled"))
)

compared = (
    summarized_rows.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat_filled") == F.col("g.max_csat_filled"))
    .select(
        "s.*",
        "g.max_csat_filled",
        F.round(F.col("s.csat_filled") - F.col("g.max_csat_filled"), 2).alias("csat_filled_minus_max"),
    )
)

derived = (
    compared
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.initcap(F.trim(F.col("priority_group"))).alias("priority_group_clean"),
        F.abs(F.col("prev3_closed_at_hour") - 10).alias("prev3_closed_at_hour_distance"),
    )
)

result = (
    derived
    .groupBy("closed_at", "event_count")
    .pivot("channel", ["web", "email", "chat", "phone"])
    .agg(F.max("csat"))
    .orderBy("closed_at", "event_count")
)
