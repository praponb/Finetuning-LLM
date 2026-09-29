from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_by_minutes_ev = Window.partitionBy("ticket_channel").orderBy(F.col("minutes_event_at_day_to_first_event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_channel_by_minutes_ev_2 = Window.partitionBy("ticket_channel").orderBy(F.col("minutes_event_at_day_to_first_event_at").asc_nulls_last())

recent_events = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_counts = (
    recent_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.last_event_at")
)

tagged = (
    with_counts
    .select(
        "*",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor_group")).alias("event_type_actor_group_label"),
        F.round((F.unix_timestamp(F.col("first_event_at")) - F.unix_timestamp(F.col("event_at_day"))) / 60, 1).alias("minutes_event_at_day_to_first_event_at"),
        F.when(F.col("actor_group").isin("other", "group_a"), "group_a").when(F.col("actor_group") == "group_b", "group_b").otherwise("other").alias("actor_group_group"),
    )
)

combined_details = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.channel").alias("ticket_channel"))
)

with_windows = (
    combined_details
    .withColumn("second_first_event_at", F.nth_value("first_event_at", 2).over(w_ticket_channel_by_minutes_ev))
    .withColumn("minutes_event_at_day_to_first_event_at_rank", F.rank().over(w_ticket_channel_by_minutes_ev_2))
)

result = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_day",
        "actor_group",
        "first_event_at",
        "last_event_at",
        "event_type_actor_group_label",
        "minutes_event_at_day_to_first_event_at",
        "second_first_event_at",
        "minutes_event_at_day_to_first_event_at_rank",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
    .orderBy(
        F.col("event_type_actor_group_label").desc(),
        F.col("ticket_id").desc(),
        "id",
    )
    .limit(20)
)
