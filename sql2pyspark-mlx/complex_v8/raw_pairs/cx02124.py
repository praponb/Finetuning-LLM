from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at_week").desc_nulls_first())
w_all_by_event_at_week = Window.orderBy("event_at_week", "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at_week").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_week_full = Window.orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_ticket_events
    .filter(~F.col("event_type").isin("escalated", "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

windowed = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "event_type_actor_label",
        "event_at_week",
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at), 3).alias("event_at_week_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_event_at_week), 3).alias("event_at_week_percent_rank"),
        F.nth_value("event_at", 2).over(w_ticket_id_actor_by_event_at_2).alias("second_event_at"),
        F.last("event_at_week").over(w_all_by_event_at_week_full).alias("last_event_at_week"),
    )
)

without_matches = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system"), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "event_type_actor_label",
        "event_at_week_cume_dist",
        "event_at_week_percent_rank",
        "second_event_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("second_event_at")).alias("days_since_second_event_at"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
    .orderBy(F.col("days_since_second_event_at").asc_nulls_last(), "id")
    .limit(20)
)
