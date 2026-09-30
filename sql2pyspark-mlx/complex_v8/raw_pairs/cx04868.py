from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_event_at = Window.orderBy("days_since_event_at", "id")
w_ticket_id_ticket_priority = Window.partitionBy("ticket_id", "ticket_priority")
w_ticket_id_event_type_by_tick = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("ticket_csat").desc_nulls_first())
w_event_type_ticket_csat_by_da = Window.partitionBy("event_type", "ticket_csat").orderBy(F.col("days_since_event_at").desc_nulls_first(), "id")
w_event_type_ticket_csat_by_da_2 = Window.partitionBy("event_type", "ticket_csat").orderBy(F.col("days_since_event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_tick_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("ticket_csat").desc_nulls_first(), "id")
w_all_by_days_since_event_at_t = Window.orderBy("days_since_event_at")
w_ticket_id_event_type_by_tick_3 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("ticket_csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_event_at_m = Window.orderBy("days_since_event_at", "id").rowsBetween(-1, Window.currentRow)
w_all_by_days_since_event_at_r = Window.orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev3_event_at = Window.orderBy(F.col("prev3_event_at").desc(), "id")
w_distinct_event_type_in_ticke = Window.partitionBy("distinct_event_type_in_ticket_id_ticket_priority").orderBy(F.col("ticket_csat_dense_rank").desc(), "id").rowsBetween(-3, Window.currentRow)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last())
w_distinct_event_type_in_ticke_2 = Window.partitionBy("distinct_event_type_in_ticket_id_ticket_priority").orderBy(F.col("ticket_csat_dense_rank").desc(), "id")
w_distinct_event_type_in_ticke_3 = Window.partitionBy("distinct_event_type_in_ticket_id_ticket_priority").orderBy(F.col("ticket_csat_dense_rank").desc())
w_ticket_csat_dense_rank = Window.partitionBy("ticket_csat_dense_rank")

cleaned_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(10, 13)) & (F.col("actor").isin("agent", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

combined_details_ticket = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.days_since_event_at",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

windowed = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "days_since_event_at",
        "ticket_csat",
        "ticket_priority",
        F.lag("event_at", 3).over(w_all_by_days_since_event_at).alias("prev3_event_at"),
        F.size(F.collect_set("event_type").over(w_ticket_id_ticket_priority)).alias("distinct_event_type_in_ticket_id_ticket_priority"),
        F.rank().over(w_ticket_id_event_type_by_tick).alias("ticket_csat_rank"),
        F.round(F.percent_rank().over(w_event_type_ticket_csat_by_da), 3).alias("days_since_event_at_percent_rank"),
        F.nth_value("event_at", 2).over(w_event_type_ticket_csat_by_da_2).alias("second_event_at"),
        F.dense_rank().over(w_ticket_id_event_type_by_tick_2).alias("ticket_csat_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_days_since_event_at_t), 3).alias("days_since_event_at_cume_dist"),
        F.last("event_at").over(w_ticket_id_event_type_by_tick_3).alias("last_event_at"),
        F.lead("days_since_event_at", 1, 0).over(w_all_by_days_since_event_at).alias("next_days_since_event_at"),
        F.count(F.lit(1)).over(w_ticket_id_ticket_priority).alias("rows_in_ticket_id_ticket_priority"),
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_ticket_id_ticket_priority), 2).alias("days_since_event_at_vs_ticket_id_ticket_priority"),
        F.max("ticket_csat").over(w_all_by_days_since_event_at_m).alias("ticket_csat_max_2"),
        F.count(F.lit(1)).over(w_all_by_days_since_event_at_r).alias("running_count"),
    )
)

enriched = (
    windowed
    .filter(F.col("days_since_event_at") < 455)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_csat",
        "ticket_priority",
        "prev3_event_at",
        "distinct_event_type_in_ticket_id_ticket_priority",
        "second_event_at",
        "ticket_csat_dense_rank",
        "days_since_event_at_cume_dist",
        "next_days_since_event_at",
        "rows_in_ticket_id_ticket_priority",
        "running_count",
        F.round((F.unix_timestamp(F.col("second_event_at")) - F.unix_timestamp(F.col("prev3_event_at"))) / 3600, 1).alias("hours_prev3_event_at_to_second_event_at"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_prev3_event_at), 3).alias("prev3_event_at_percent_rank"),
        F.round(F.avg("next_days_since_event_at").over(w_distinct_event_type_in_ticke), 2).alias("next_days_since_event_at_avg_4"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.row_number().over(w_distinct_event_type_in_ticke_2).alias("ticket_csat_dense_rank_seq"),
        F.round(F.cume_dist().over(w_distinct_event_type_in_ticke_3), 3).alias("ticket_csat_dense_rank_cume_dist"),
        F.max("next_days_since_event_at").over(w_ticket_csat_dense_rank).alias("ticket_csat_dense_rank_max_next_days_since_event"),
        F.round((F.unix_timestamp("second_event_at") - F.unix_timestamp(F.lag("second_event_at").over(w_distinct_event_type_in_ticke_2))) / 60, 1).alias("minutes_since_prev_second_event_at"),
    )
)

result = (
    metrics
    .filter(F.col("distinct_event_type_in_ticket_id_ticket_priority") >= 3)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_csat",
        "ticket_priority",
        "second_event_at",
        "days_since_event_at_cume_dist",
        "next_days_since_event_at",
        "rows_in_ticket_id_ticket_priority",
        "running_count",
        "prev3_event_at_percent_rank",
        "next_days_since_event_at_avg_4",
        "event_at_percent_rank",
        "ticket_csat_dense_rank_cume_dist",
        "minutes_since_prev_second_event_at",
        F.when(F.col("ticket_priority").isin("P2", "P4"), "group_a").when(F.col("ticket_priority") == "P1", "group_b").otherwise("other").alias("ticket_priority_group"),
        F.lower(F.trim(F.col("ticket_priority"))).alias("ticket_priority_clean"),
    )
    .orderBy(F.col("id").desc(), F.col("next_days_since_event_at_avg_4").desc())
    .limit(10)
)
