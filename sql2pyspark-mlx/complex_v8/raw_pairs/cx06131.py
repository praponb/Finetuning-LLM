from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_ticket_priority_by = Window.partitionBy("ticket_id", "ticket_priority").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_event_at_month_by = Window.partitionBy("ticket_id", F.month("event_at")).orderBy("event_at_plus_2h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_plus_2h_month_event = Window.partitionBy(F.date_trunc("month", "event_at_plus_2h"), F.month("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at_plus_2h").asc_nulls_last())
w_ticket_id_ticket_priority_by_2 = Window.partitionBy("ticket_id", "ticket_priority").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at_plus_2h", "id")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at_plus_2h").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_plus_2h_month_event_2 = Window.partitionBy(F.date_trunc("month", "event_at_plus_2h"), F.month("event_at")).orderBy(F.col("event_at").desc(), "id")
w_ticket_id_ticket_priority_by_3 = Window.partitionBy("ticket_id", "ticket_priority").orderBy(F.col("event_at").desc())
w_second_event_at_to_date = Window.partitionBy(F.to_date("second_event_at"))
w_ticket_id_by_prev2_event_at = Window.partitionBy("ticket_id").orderBy(F.col("prev2_event_at_plus_2h").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_ticket_id_by_prev2_event_at_2 = Window.partitionBy("ticket_id").orderBy(F.col("prev2_event_at_plus_2h").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_prev2_event_at_3 = Window.partitionBy("ticket_id").orderBy(F.col("prev2_event_at_plus_2h").asc_nulls_last(), "id")

base_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")) & (~F.col("event_type").isin("resolved", "reopened")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
)

combined_details_ticket = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_at_plus_2h",
        F.col("t.priority").alias("ticket_priority"),
    )
)

metrics = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_priority",
        F.lag("event_at").over(w_ticket_id_ticket_priority_by).alias("prev_event_at"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_event_at_month_by).alias("last_known_event_at"),
        F.nth_value("event_at_plus_2h", 2).over(w_event_at_plus_2h_month_event).alias("second_event_at_plus_2h"),
        F.round(F.percent_rank().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_plus_2h_percent_rank"),
        F.nth_value("event_at", 2).over(w_ticket_id_ticket_priority_by_2).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at_plus_2h") - F.unix_timestamp(F.lag("event_at_plus_2h").over(w_ticket_id_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_plus_2h"),
        F.lag("event_at_plus_2h", 2).over(w_ticket_id_ticket_priority_by).alias("prev2_event_at_plus_2h"),
        F.first("event_at_plus_2h", ignorenulls=True).over(w_actor_ticket_id_by_event_at_2).alias("first_known_event_at_plus_2h"),
        F.nth_value("event_at_plus_2h", 2).over(w_ticket_id_ticket_priority_by_2).alias("second_event_at_plus_2h_3"),
        F.ntile(10).over(w_event_at_plus_2h_month_event_2).alias("event_at_decile"),
        F.dense_rank().over(w_ticket_id_actor_by_event_at).alias("event_at_plus_2h_dense_rank"),
        F.round(F.cume_dist().over(w_ticket_id_ticket_priority_by_3), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_ticket_id_ticket_priority_by_3).alias("event_at_dense_rank"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.count(F.lit(1)).over(w_second_event_at_to_date).alias("rows_in_second_event_at_to_date"),
        F.max("event_at_cume_dist").over(w_ticket_id_by_prev2_event_at).alias("event_at_cume_dist_max_2_prior"),
        F.first("second_event_at_plus_2h", ignorenulls=True).over(w_ticket_id_by_prev2_event_at_2).alias("first_known_second_event_at_plus_2h"),
        F.round(F.avg("event_at_cume_dist").over(w_second_event_at_to_date), 2).alias("second_event_at_to_date_avg_event_at_cume_dist"),
        F.lead("event_at_decile", 1, 0).over(w_ticket_id_by_prev2_event_at_3).alias("next_event_at_decile"),
        F.round(F.avg("event_at_decile").over(w_second_event_at_to_date), 2).alias("second_event_at_to_date_avg_event_at_decile"),
    )
)

result = (
    metrics_2
    .filter(F.hour(F.col("event_at")).between(10, 12))
    .select(
        "id",
        "event_at",
        "ticket_priority",
        "prev_event_at",
        "last_known_event_at",
        "second_event_at_plus_2h",
        "event_at_plus_2h_percent_rank",
        "second_event_at",
        "minutes_since_prev_event_at_plus_2h",
        "first_known_event_at_plus_2h",
        "second_event_at_plus_2h_3",
        "event_at_decile",
        "event_at_plus_2h_dense_rank",
        "event_at_cume_dist",
        "rows_in_second_event_at_to_date",
        "event_at_cume_dist_max_2_prior",
        "second_event_at_to_date_avg_event_at_cume_dist",
        "next_event_at_decile",
        "second_event_at_to_date_avg_event_at_decile",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
    .orderBy(
        F.col("second_event_at_plus_2h_3").desc_nulls_first(),
        F.col("ticket_priority").asc_nulls_last(),
    )
)
