from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_min_ticket_csat_by_min_t = Window.partitionBy("cnt", "min_ticket_csat").orderBy("min_ticket_csat", "event_count", "ticket_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cnt_min_ticket_csat_by_min_t_2 = Window.partitionBy("cnt", "min_ticket_csat").orderBy("min_ticket_csat", "event_count", "ticket_id").rowsBetween(-3, Window.currentRow)
w_event_count = Window.partitionBy("event_count")
w_cnt_min_ticket_csat_by_min_t_3 = Window.partitionBy("cnt", "min_ticket_csat").orderBy("min_ticket_csat", "event_count", "ticket_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_min_ticket_csat_ties = Window.orderBy("min_ticket_csat")

recent = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type").isin("assigned", "replied")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_details_ticket = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.actor_or_unknown",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

annotated = (
    with_details_ticket
    .filter(F.month(F.col("ticket_closed_at")).isin(1, 2))
    .select(
        "*",
        F.date_format(F.col("ticket_closed_at"), "MMM yyyy").alias("ticket_closed_at_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("ticket_closed_at")).alias("days_since_ticket_closed_at"),
        F.concat_ws("|", F.col("actor"), F.col("actor_or_unknown")).alias("actor_actor_or_unknown_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    annotated.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

totals = (
    with_counts
    .groupBy("event_count", "ticket_id")
    .agg(
        F.round(F.sum("ticket_csat") / F.nullif(F.sum("ticket_requester_id"), F.lit(0)), 3).alias("ticket_csat_to_ticket_requester_id_ratio"),
        F.percentile_approx("event_count", 0.5).alias("median_event_count"),
        F.min("ticket_csat").alias("min_ticket_csat"),
        F.count("*").alias("cnt"),
    )
)

sequenced = (
    totals
    .select(
        "event_count",
        "ticket_id",
        "ticket_csat_to_ticket_requester_id_ratio",
        "min_ticket_csat",
        F.sum("cnt").over(w_cnt_min_ticket_csat_by_min_t).alias("running_cnt"),
        F.round(F.avg("event_count").over(w_cnt_min_ticket_csat_by_min_t_2), 2).alias("event_count_avg_4"),
        F.count(F.lit(1)).over(w_event_count).alias("rows_in_event_count"),
        F.round(F.col("event_count") / F.nullif(F.sum("event_count").over(w_event_count), F.lit(0)), 4).alias("event_count_share_of_event_count"),
        F.nth_value("median_event_count", 2).over(w_cnt_min_ticket_csat_by_min_t_3).alias("second_median_event_count"),
        F.round(F.percent_rank().over(w_all_by_min_ticket_csat_ties), 3).alias("min_ticket_csat_percent_rank"),
    )
)

result = (
    sequenced
    .filter(F.col("rows_in_event_count") <= 15)
    .select(
        "event_count",
        "ticket_csat_to_ticket_requester_id_ratio",
        "min_ticket_csat",
        "running_cnt",
        "event_count_avg_4",
        "rows_in_event_count",
        "event_count_share_of_event_count",
        "second_median_event_count",
        "min_ticket_csat_percent_rank",
        F.greatest(F.col("running_cnt"), F.col("event_count")).alias("greatest_running_cnt_event_count"),
    )
    .orderBy("rows_in_event_count", F.col("running_cnt").desc())
)
