from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_group_ticket_id_b = Window.partitionBy("event_type_group", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_group_ticket_id_b_2 = Window.partitionBy("event_type_group", "ticket_id").orderBy("event_at")
w_event_type_group_ticket_id_b_3 = Window.partitionBy("event_type_group", "ticket_id").orderBy("event_at", "id")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")

valid = (
    df_ticket_events
    .filter(F.col("event_type") == "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "replied"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.when(F.col("event_type").isin("replied", "resolved"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group_2"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.last("event_at").over(w_event_type_group_ticket_id_b).alias("last_event_at"),
        F.round(F.cume_dist().over(w_event_type_group_ticket_id_b_2), 3).alias("event_at_cume_dist"),
        F.ntile(3).over(w_event_type_group_ticket_id_b_3).alias("event_at_tercile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (~F.col("t.priority").isin("P3", "P4")), "left_semi")
)

exploded_detail = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "assigned"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        "s.actor_prefix",
        "s.event_at_cume_dist",
        "s.event_at_tercile",
        "s.minutes_since_prev_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    exploded_detail.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group",
        "actor_prefix",
        "event_id",
        "event_event_type",
        "event_actor",
        "event_count",
    )
    .orderBy("event_type")
)
