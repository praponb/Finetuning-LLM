from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_csat_ticket_id_by_tic = Window.partitionBy("ticket_csat", "ticket_id").orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_ticket_id_by_tic_2 = Window.partitionBy("ticket_csat", "ticket_id").orderBy("ticket_opened_at", "id")
w_ticket_csat_ticket_id_by_tic_3 = Window.partitionBy("ticket_csat", "ticket_id").orderBy("ticket_opened_at")
w_ticket_opened_at_rank_event = Window.partitionBy("ticket_opened_at_rank", "event_type")
w_all_by_ticket_opened_at_runn = Window.orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_event_type_in_ticke = Window.partitionBy("distinct_event_type_in_ticket_id").orderBy(F.col("rn").desc_nulls_first(), "id")

scoped_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.min("ticket_csat").over(w_ticket_id).alias("ticket_id_min_ticket_csat"),
        F.nth_value("ticket_opened_at", 2).over(w_ticket_csat_ticket_id_by_tic).alias("second_ticket_opened_at"),
        F.row_number().over(w_ticket_csat_ticket_id_by_tic_2).alias("rn"),
        F.size(F.collect_set("event_type").over(w_ticket_id)).alias("distinct_event_type_in_ticket_id"),
        F.rank().over(w_ticket_csat_ticket_id_by_tic_3).alias("ticket_opened_at_rank"),
        F.max("ticket_csat").over(w_ticket_id).alias("ticket_id_max_ticket_csat"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_opened_at_rank_event), 2).alias("ticket_csat_vs_ticket_opened_at_rank_event_type_"),
        F.last("second_ticket_opened_at", ignorenulls=True).over(w_all_by_ticket_opened_at_runn).alias("last_known_second_ticket_opened_at"),
        (F.col("ticket_id_max_ticket_csat") - F.lag("ticket_id_max_ticket_csat").over(w_distinct_event_type_in_ticke)).alias("ticket_id_max_ticket_csat_change"),
        F.sum("ticket_id_min_ticket_csat").over(w_ticket_opened_at_rank_event).alias("ticket_opened_at_rank_event_type_sum_ticket_id_m"),
    )
)

detail_rows_events = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.ticket_csat",
        "s.ticket_opened_at",
        "s.rn",
        "s.distinct_event_type_in_ticket_id",
        "s.ticket_opened_at_rank",
        "s.ticket_id_max_ticket_csat",
        "s.ticket_csat_vs_ticket_opened_at_rank_event_type_",
        "s.last_known_second_ticket_opened_at",
        "s.ticket_id_max_ticket_csat_change",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.event_type").alias("event_event_type"),
    )
)

totals = (
    detail_rows_events
    .groupBy(
        "distinct_event_type_in_ticket_id",
        F.date_trunc("month", "ticket_opened_at").alias("ticket_opened_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.sum("ticket_csat") / F.nullif(F.sum("ticket_id_max_ticket_csat_change"), F.lit(0)), 3).alias("ticket_csat_to_ticket_id_max_ticket_csat_change_"),
        F.sum("distinct_event_type_in_ticket_id").alias("total_distinct_event_type_in_ticket_id"),
        F.sum(F.when(F.col("event_type").isNotNull(), 1).otherwise(0)).alias("n_event_type_is_not_null"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    totals
    .select(
        "distinct_event_type_in_ticket_id",
        "ticket_opened_at_month",
        "event_type_list",
        "ticket_csat_to_ticket_id_max_ticket_csat_change_",
        "total_distinct_event_type_in_ticket_id",
        "n_event_type_is_not_null",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("ticket_opened_at_month")).alias("days_since_ticket_opened_at_month"),
        (F.floor(F.col("n_event_type_is_not_null") / 2) * 2).alias("n_event_type_is_not_null_bucket"),
    )
    .orderBy(
        F.col("n_event_type_is_not_null_bucket").asc(),
        F.col("ticket_opened_at_month").asc_nulls_last(),
    )
)
