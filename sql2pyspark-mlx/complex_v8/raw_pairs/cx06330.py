from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_agent_id_b = Window.partitionBy("event_type", "ticket_agent_id").orderBy("ticket_csat", "id")
w_all_by_ticket_csat_ties = Window.orderBy("ticket_csat")
w_ticket_csat_event_type_by_ti = Window.partitionBy("ticket_csat", "event_type").orderBy(F.col("ticket_csat").desc_nulls_first(), "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_csat_by_ticket_csat_p = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_csat").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_ticket_csat_by_ticket_csat = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_csat").asc_nulls_last(), "id")
w_event_type_by_ticket_csat_fu = Window.partitionBy("event_type").orderBy(F.col("ticket_csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_all_by_ticket_csat = Window.orderBy("ticket_csat", "id")
w_event_type_by_ticket_csat = Window.partitionBy("event_type").orderBy(F.col("ticket_csat").asc_nulls_last(), "id")
w_all_by_ticket_csat_full = Window.orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_ticket_csat_2_by_tick = Window.partitionBy("second_ticket_csat_2").orderBy(F.col("ticket_csat_avg_3_prior").desc(), "id")
w_second_ticket_csat_2_by_tick_2 = Window.partitionBy("second_ticket_csat_2").orderBy(F.col("ticket_csat_avg_3_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_agent_id_by_ticket_cs = Window.partitionBy("ticket_agent_id").orderBy("ticket_csat_dense_rank_2", "id").rowsBetween(-2, Window.currentRow)
w_ticket_csat = Window.partitionBy("ticket_csat")

scoped_events = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(6, 10))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

metrics = (
    enriched_ticket
    .select(
        "*",
        F.row_number().over(w_event_type_ticket_agent_id_b).alias("seq_no"),
        F.round(F.cume_dist().over(w_all_by_ticket_csat_ties), 3).alias("ticket_csat_cume_dist"),
        F.dense_rank().over(w_ticket_csat_event_type_by_ti).alias("ticket_csat_dense_rank"),
        F.min("ticket_csat").over(w_ticket_id).alias("ticket_id_min_ticket_csat"),
        F.round(F.avg("ticket_csat").over(w_ticket_csat_by_ticket_csat_p), 2).alias("ticket_csat_avg_3_prior"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_id), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_id"),
        F.dense_rank().over(w_ticket_csat_by_ticket_csat).alias("ticket_csat_dense_rank_2"),
        F.nth_value("ticket_csat", 2).over(w_event_type_by_ticket_csat_fu).alias("second_ticket_csat"),
        F.size(F.collect_set("event_type").over(w_ticket_id)).alias("distinct_event_type_in_ticket_id"),
        F.round(F.avg("ticket_csat").over(w_ticket_id_event_type), 2).alias("ticket_id_event_type_avg_ticket_csat"),
        (F.col("ticket_csat") - F.lag("ticket_csat").over(w_all_by_ticket_csat)).alias("ticket_csat_change"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_ticket_csat))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("ticket_csat", 2).over(w_all_by_ticket_csat_full).alias("second_ticket_csat_2"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.lag("ticket_id_min_ticket_csat").over(w_second_ticket_csat_2_by_tick).alias("prev_ticket_id_min_ticket_csat"),
        F.nth_value("ticket_csat_dense_rank", 2).over(w_second_ticket_csat_2_by_tick_2).alias("second_ticket_csat_dense_rank"),
        F.sum("ticket_csat_change").over(w_ticket_agent_id_by_ticket_cs).alias("ticket_csat_change_sum_3"),
        F.round(F.col("ticket_id_min_ticket_csat") / F.nullif(F.sum("ticket_id_min_ticket_csat").over(w_ticket_csat), F.lit(0)), 4).alias("ticket_id_min_ticket_csat_share_of_ticket_csat"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_agent_id",
        "ticket_csat",
        "seq_no",
        "ticket_csat_cume_dist",
        "ticket_id_min_ticket_csat",
        "ticket_csat_share_of_ticket_id",
        "ticket_csat_dense_rank_2",
        "second_ticket_csat",
        "distinct_event_type_in_ticket_id",
        "ticket_csat_change",
        "minutes_since_prev_event_at",
        "second_ticket_csat_2",
        "second_ticket_csat_dense_rank",
        "ticket_csat_change_sum_3",
        "ticket_id_min_ticket_csat_share_of_ticket_csat",
        F.when(F.col("ticket_csat_dense_rank") >= 9, "large").when(F.col("ticket_csat_dense_rank") >= 3, "medium").otherwise("small").alias("ticket_csat_dense_rank_tier"),
    )
)
