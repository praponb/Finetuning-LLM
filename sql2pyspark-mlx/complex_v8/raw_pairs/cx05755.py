from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_ticket_csat_by_tic = Window.partitionBy("ticket_id", "ticket_csat").orderBy("ticket_requester_id", "id").rowsBetween(-6, -1)
w_event_type_ticket_csat_by_ev = Window.partitionBy("event_type", "ticket_csat").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor = Window.partitionBy("actor")
w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_csat_event_type_by_ev = Window.partitionBy("ticket_csat", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_csat_by_ticket_reques = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_requester_id").desc(), "id")
w_ticket_id_ticket_csat_by_tic_2 = Window.partitionBy("ticket_id", "ticket_csat").orderBy("ticket_requester_id", "id").rowsBetween(-3, -1)
w_ticket_csat_by_ticket_reques_2 = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_csat_by_ticket_reques_3 = Window.partitionBy("ticket_csat").orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_by_ticket_reques_4 = Window.partitionBy("ticket_csat").orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_csat_event_type_by_ev_2 = Window.partitionBy("ticket_csat", "event_type").orderBy("event_at")
w_distinct_event_type_in_actor = Window.partitionBy("distinct_event_type_in_actor")
w_cnt_distinct_event_type_in_a = Window.partitionBy("cnt", "distinct_event_type_in_actor").orderBy("last_first_known_event_at", "distinct_event_type_in_actor", "last_known_event_at_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 48) & (F.col("event_type") != "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") < F.lit("2025-02-06 15:00:00").cast("timestamp")), "inner")
    .filter(F.col("t.requester_id") < 21)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_actor_label",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_actor_label",
        "ticket_requester_id",
        F.max("ticket_csat").over(w_ticket_id_ticket_csat_by_tic).alias("ticket_csat_max_6_prior"),
        F.min("ticket_csat").over(w_event_type_ticket_csat_by_ev).alias("min_ticket_csat_so_far"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_actor), F.lit(0)), 4).alias("ticket_csat_share_of_actor"),
        F.max("ticket_csat").over(w_ticket_id).alias("ticket_id_max_ticket_csat"),
        F.sum("ticket_csat").over(w_event_type_ticket_csat_by_ev).alias("running_ticket_csat"),
        F.last("event_at", ignorenulls=True).over(w_ticket_csat_event_type_by_ev).alias("last_known_event_at"),
        (F.col("ticket_requester_id") - F.lag("ticket_requester_id").over(w_ticket_csat_by_ticket_reques)).alias("ticket_requester_id_change"),
        F.round(F.avg("ticket_csat").over(w_ticket_id), 2).alias("ticket_id_avg_ticket_csat"),
        F.size(F.collect_set("event_type").over(w_actor)).alias("distinct_event_type_in_actor"),
        F.round(F.avg("ticket_csat").over(w_ticket_id_ticket_csat_by_tic_2), 2).alias("ticket_csat_avg_3_prior"),
        F.first("event_at", ignorenulls=True).over(w_ticket_csat_by_ticket_reques_2).alias("first_known_event_at"),
        F.nth_value("ticket_csat", 2).over(w_ticket_csat_by_ticket_reques_3).alias("second_ticket_csat"),
        F.first("ticket_requester_id", ignorenulls=True).over(w_ticket_csat_by_ticket_reques_4).alias("first_known_ticket_requester_id"),
        F.min("ticket_csat").over(w_actor).alias("actor_min_ticket_csat"),
        F.dense_rank().over(w_ticket_csat_event_type_by_ev_2).alias("event_at_dense_rank"),
    )
)

per_group = (
    sequenced
    .groupBy(
        "distinct_event_type_in_actor",
        F.date_format("last_known_event_at", "yyyy-MM").alias("last_known_event_at_ym"),
    )
    .agg(
        F.max("first_known_event_at").alias("last_first_known_event_at"),
        F.count("*").alias("cnt"),
    )
)

sequenced_2 = (
    per_group
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_event_type_in_actor).alias("rows_in_distinct_event_type_in_actor"),
        F.last("last_first_known_event_at").over(w_cnt_distinct_event_type_in_a).alias("last_last_first_known_event_at"),
    )
)

result = (
    sequenced_2
    .select(
        "distinct_event_type_in_actor",
        "last_known_event_at_ym",
        "last_first_known_event_at",
        "cnt",
        "last_last_first_known_event_at",
    )
    .orderBy("last_first_known_event_at")
)
