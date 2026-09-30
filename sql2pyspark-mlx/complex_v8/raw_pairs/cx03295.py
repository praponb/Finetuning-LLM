from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_csat_by_ev = Window.partitionBy("event_type", "ticket_csat").orderBy("event_at", "id")
w_ticket_id_ticket_csat = Window.partitionBy("ticket_id", "ticket_csat")
w_ticket_csat_actor_by_ticket = Window.partitionBy("ticket_csat", "actor").orderBy(F.col("ticket_requester_id").desc_nulls_first(), "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_csat_event_at_month_b = Window.partitionBy("ticket_csat", F.month("event_at")).orderBy("ticket_requester_id", "id")
w_ticket_csat_event_at_month_b_2 = Window.partitionBy("ticket_csat", F.month("event_at")).orderBy("event_at")
w_ticket_csat_event_at_month_b_3 = Window.partitionBy("ticket_csat", F.month("event_at")).orderBy("event_at", "id").rowsBetween(-3, Window.currentRow)
w_event_at_seq_by_prev3_ticket = Window.partitionBy("event_at_seq").orderBy(F.col("prev3_ticket_csat").asc_nulls_last())
w_ticket_csat = Window.partitionBy("ticket_csat")
w_event_at_seq_by_prev3_ticket_2 = Window.partitionBy("event_at_seq").orderBy(F.col("prev3_ticket_csat").asc_nulls_last(), "id")
w_event_at_seq_by_prev3_ticket_3 = Window.partitionBy("event_at_seq").orderBy(F.col("prev3_ticket_csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 38)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_details = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_clean",
        "s.days_since_event_at",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.row_number().over(w_event_type_ticket_csat_by_ev).alias("event_at_seq"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_id_ticket_csat), 2).alias("ticket_csat_vs_ticket_id_ticket_csat_avg"),
        F.round(F.avg("ticket_requester_id").over(w_ticket_id_ticket_csat), 2).alias("ticket_id_ticket_csat_avg_ticket_requester_id"),
        F.ntile(10).over(w_ticket_csat_actor_by_ticket).alias("ticket_requester_id_decile"),
        F.size(F.collect_set("actor").over(w_ticket_id)).alias("distinct_actor_in_ticket_id"),
        F.max("days_since_event_at").over(w_ticket_id_ticket_csat).alias("ticket_id_ticket_csat_max_days_since_event_at"),
        F.lag("event_at").over(w_ticket_csat_actor_by_ticket).alias("prev_event_at"),
        F.rank().over(w_event_type_ticket_csat_by_ev).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_ticket_csat_event_at_month_b), 3).alias("ticket_requester_id_cume_dist"),
        F.ntile(5).over(w_ticket_csat_event_at_month_b).alias("ticket_requester_id_quintile"),
        F.round(F.percent_rank().over(w_ticket_csat_event_at_month_b_2), 3).alias("event_at_percent_rank"),
        F.lag("ticket_csat", 3).over(w_event_type_ticket_csat_by_ev).alias("prev3_ticket_csat"),
        F.max("ticket_requester_id").over(w_ticket_csat_event_at_month_b_3).alias("ticket_requester_id_max_4"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        (F.floor(F.col("ticket_requester_id") / 5) * 5).alias("ticket_requester_id_bucket"),
        (F.col("event_at_percent_rank") - F.col("event_at_seq")).alias("event_at_percent_rank_minus_event_at_seq"),
        F.when(F.col("event_type").isin("replied", "resolved"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
    )
)

windowed_2 = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_clean",
        "days_since_event_at",
        "ticket_requester_id",
        "ticket_csat",
        "event_at_seq",
        "ticket_csat_vs_ticket_id_ticket_csat_avg",
        "ticket_id_ticket_csat_avg_ticket_requester_id",
        "distinct_actor_in_ticket_id",
        "ticket_id_ticket_csat_max_days_since_event_at",
        "prev_event_at",
        "event_at_rank",
        "ticket_requester_id_cume_dist",
        "event_at_percent_rank",
        "prev3_ticket_csat",
        "ticket_requester_id_max_4",
        "ticket_requester_id_bucket",
        "event_type_group",
        F.round(F.percent_rank().over(w_event_at_seq_by_prev3_ticket), 3).alias("prev3_ticket_csat_percent_rank"),
        F.size(F.collect_set("event_type").over(w_ticket_csat)).alias("distinct_event_type_in_ticket_csat"),
        F.count(F.lit(1)).over(w_ticket_csat).alias("rows_in_ticket_csat"),
        F.size(F.collect_set("ticket_id").over(w_ticket_csat)).alias("distinct_ticket_id_in_ticket_csat"),
        F.lead("prev3_ticket_csat").over(w_event_at_seq_by_prev3_ticket_2).alias("next_prev3_ticket_csat"),
        F.last("ticket_requester_id_quintile").over(w_event_at_seq_by_prev3_ticket_3).alias("last_ticket_requester_id_quintile"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_clean",
        "days_since_event_at",
        "ticket_requester_id",
        "ticket_csat",
        "ticket_csat_vs_ticket_id_ticket_csat_avg",
        "ticket_id_ticket_csat_avg_ticket_requester_id",
        "distinct_actor_in_ticket_id",
        "ticket_id_ticket_csat_max_days_since_event_at",
        "event_at_rank",
        "prev3_ticket_csat",
        "ticket_requester_id_max_4",
        "prev3_ticket_csat_percent_rank",
        "next_prev3_ticket_csat",
        "last_ticket_requester_id_quintile",
        (F.floor(F.col("event_at_seq") / 2) * 2).alias("event_at_seq_bucket"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
    .orderBy(F.col("id").asc_nulls_last())
    .limit(10)
)
