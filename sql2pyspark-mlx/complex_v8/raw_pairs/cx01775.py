from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_ticket_chann = Window.partitionBy("ticket_priority", "ticket_channel").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_channel_ticket_id_by = Window.partitionBy("ticket_channel", "ticket_id").orderBy("ticket_closed_at", "id")
w_all_by_ticket_closed_at_full = Window.orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_priority_ticket_chann_2 = Window.partitionBy("ticket_priority", "ticket_channel").orderBy("ticket_closed_at")
w_all_by_ticket_closed_at = Window.orderBy("ticket_closed_at", "id")
w_ticket_closed_at_decile_tick = Window.partitionBy("ticket_closed_at_decile", "ticket_channel")
w_all_by_prev_ticket_closed_at = Window.orderBy(F.col("prev_ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_ticket_closed_at_mont = Window.partitionBy(F.month("second_ticket_closed_at"), "ticket_channel").orderBy("ticket_closed_at_cume_dist", "id")
w_all_by_second_ticket_closed = Window.orderBy("second_ticket_closed_at_2", "id")

base_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("ticket_id") <= 9))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.when(F.col("event_type").isin("created", "resolved"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
)

enriched_ticket = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor_group",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_windows = (
    enriched_ticket
    .select(
        "*",
        F.nth_value("ticket_closed_at", 2).over(w_ticket_priority_ticket_chann).alias("second_ticket_closed_at"),
        F.ntile(10).over(w_ticket_channel_ticket_id_by).alias("ticket_closed_at_decile"),
        F.nth_value("ticket_closed_at", 2).over(w_all_by_ticket_closed_at_full).alias("second_ticket_closed_at_2"),
        F.round(F.percent_rank().over(w_ticket_priority_ticket_chann_2), 3).alias("ticket_closed_at_percent_rank"),
        F.row_number().over(w_all_by_ticket_closed_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_ticket_priority_ticket_chann_2), 3).alias("ticket_closed_at_cume_dist"),
        F.row_number().over(w_ticket_channel_ticket_id_by).alias("seq_no_2"),
        F.lag("ticket_closed_at").over(w_ticket_channel_ticket_id_by).alias("prev_ticket_closed_at"),
    )
)

leaders = with_windows.filter(F.col("seq_no") == 1)

windowed = (
    leaders
    .select(
        "id",
        "actor_group",
        "ticket_priority",
        "second_ticket_closed_at",
        "ticket_closed_at_decile",
        "second_ticket_closed_at_2",
        "ticket_closed_at_percent_rank",
        "prev_ticket_closed_at",
        F.round(F.avg("ticket_closed_at_cume_dist").over(w_ticket_closed_at_decile_tick), 2).alias("ticket_closed_at_decile_ticket_channel_avg_ticke"),
        F.nth_value("ticket_closed_at_decile", 2).over(w_all_by_prev_ticket_closed_at).alias("second_ticket_closed_at_decile"),
        F.round((F.unix_timestamp("second_ticket_closed_at_2") - F.unix_timestamp(F.lag("second_ticket_closed_at_2").over(w_second_ticket_closed_at_mont))) / 60, 1).alias("minutes_since_prev_second_ticket_closed_at_2"),
        F.lead("ticket_closed_at").over(w_all_by_second_ticket_closed).alias("next_ticket_closed_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "actor_group",
        "ticket_priority",
        "second_ticket_closed_at",
        "ticket_closed_at_decile",
        "second_ticket_closed_at_2",
        "prev_ticket_closed_at",
        "ticket_closed_at_decile_ticket_channel_avg_ticke",
        "second_ticket_closed_at_decile",
        "next_ticket_closed_at",
        (F.floor(F.col("ticket_closed_at_decile") / 2) * 2).alias("ticket_closed_at_decile_bucket"),
    )
    .orderBy("actor_group")
)
