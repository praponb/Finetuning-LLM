from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_event_type_by = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last())
w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_at_month_event_type_by_2 = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type").orderBy("event_at", "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_by_event_at_week = Window.partitionBy("actor").orderBy("event_at_week", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 19)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

combined_details = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel").isNotNull()), "inner")
    .filter(F.array_contains(F.col("t.tags"), "upgrade"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_week",
        "s.event_type_slug",
        F.col("t.tags").alias("ticket_tags"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.last("event_at_week", ignorenulls=True).over(w_event_at_month_event_type_by).alias("last_known_event_at_week"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.round(F.percent_rank().over(w_event_at_month_event_type_by_2), 3).alias("event_at_percent_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_actor_by_event_at_week), 3).alias("event_at_week_percent_rank"),
        F.ntile(10).over(w_actor_by_event_at_week).alias("event_at_week_decile"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at), 3).alias("event_at_percent_rank_2"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank_2"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.row_number().over(w_event_at_month_event_type_by_2).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_week",
        "event_type_slug",
        "ticket_tags",
        "event_at_cume_dist",
        "event_at_dense_rank",
        "minutes_since_prev_event_at",
        "event_at_week_percent_rank",
        "event_at_week_decile",
        "event_at_percent_rank_2",
        "first_known_event_at",
        "seq_no",
        F.when(F.col("event_type").isin("resolved", "created"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
    .orderBy(F.col("first_known_event_at").desc(), "minutes_since_prev_event_at")
)
