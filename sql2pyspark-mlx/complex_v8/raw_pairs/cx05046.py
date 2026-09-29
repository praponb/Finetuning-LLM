from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_csat_by_ticket_csat_t = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_csat").desc_nulls_first())
w_actor = Window.partitionBy("actor")
w_all_by_ticket_csat = Window.orderBy("ticket_csat", "id")
w_ticket_csat_by_ticket_csat_f = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_by_ticket_csat = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_csat").desc_nulls_first(), "id")
w_all_by_ticket_csat_moving_2 = Window.orderBy("ticket_csat", "id").rowsBetween(-2, Window.currentRow)
w_actor_second_ticket_csat_by = Window.partitionBy("actor", "second_ticket_csat").orderBy(F.col("ticket_csat_share_of_actor").asc_nulls_last(), "id")
w_all_by_next2_event_at_prior = Window.orderBy(F.col("next2_event_at").desc(), "id").rowsBetween(-1, -1)

cleaned = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

with_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.csat") > 2)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
    )
)

metrics = (
    with_details_ticket
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "ticket_csat",
        F.dense_rank().over(w_ticket_csat_by_ticket_csat_t).alias("ticket_csat_dense_rank"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_actor), F.lit(0)), 4).alias("ticket_csat_share_of_actor"),
        F.row_number().over(w_all_by_ticket_csat).alias("seq_no"),
        F.nth_value("ticket_csat", 2).over(w_ticket_csat_by_ticket_csat_f).alias("second_ticket_csat"),
        F.min("ticket_csat").over(w_actor).alias("actor_min_ticket_csat"),
        F.size(F.collect_set("event_type").over(w_actor)).alias("distinct_event_type_in_actor"),
        F.lead("event_at", 2).over(w_ticket_csat_by_ticket_csat).alias("next2_event_at"),
        F.round(F.avg("ticket_csat").over(w_actor), 2).alias("actor_avg_ticket_csat"),
        F.row_number().over(w_ticket_csat_by_ticket_csat).alias("seq_no_2"),
        F.max("ticket_csat").over(w_actor).alias("actor_max_ticket_csat"),
        F.round(F.avg("ticket_csat").over(w_all_by_ticket_csat_moving_2), 2).alias("ticket_csat_avg_3"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.row_number().over(w_actor_second_ticket_csat_by).alias("seq_no_3"),
        F.lag("actor_min_ticket_csat").over(w_actor_second_ticket_csat_by).alias("prev_actor_min_ticket_csat"),
        F.sum("actor_min_ticket_csat").over(w_all_by_next2_event_at_prior).alias("actor_min_ticket_csat_sum_1_prior"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "ticket_csat",
        "seq_no",
        "seq_no_2",
    )
    .orderBy("ticket_csat")
)
