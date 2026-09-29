from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_csat_by_event_a = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_ticket_clo = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("ticket_closed_at", "id")
w_ticket_csat_event_type = Window.partitionBy("ticket_csat", "event_type")
w_event_at_month_by_ticket_clo_2 = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("ticket_closed_at")
w_actor_ticket_csat_by_event_a_2 = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("event_at").desc(), "id").rowsBetween(-6, -1)

prepared = (
    df_ticket_events
    .filter((F.col("event_type") != "created") & (F.col("event_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

enriched_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

with_trends = (
    enriched_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_csat",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_ticket_csat_by_event_a))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.row_number().over(w_event_at_month_by_ticket_clo).alias("ticket_closed_at_seq"),
        F.min("ticket_csat").over(w_ticket_csat_event_type).alias("ticket_csat_event_type_min_ticket_csat"),
        F.round(F.avg("ticket_csat").over(w_ticket_csat_event_type), 2).alias("ticket_csat_event_type_avg_ticket_csat"),
        F.rank().over(w_event_at_month_by_ticket_clo_2).alias("ticket_closed_at_rank"),
        F.rank().over(w_event_at_month_by_ticket_clo).alias("ticket_closed_at_rank_2"),
        F.ntile(5).over(w_event_at_month_by_ticket_clo).alias("ticket_closed_at_quintile"),
        F.count(F.lit(1)).over(w_ticket_csat_event_type).alias("rows_in_ticket_csat_event_type"),
        (F.col("ticket_csat") - F.lag("ticket_csat").over(w_event_at_month_by_ticket_clo)).alias("ticket_csat_change"),
        F.row_number().over(w_actor_ticket_csat_by_event_a).alias("event_at_seq"),
        F.max("ticket_csat").over(w_actor_ticket_csat_by_event_a_2).alias("ticket_csat_max_6_prior"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("ticket_closed_at_rank_2") <= 2)
    .drop("ticket_closed_at_rank_2")
)

per_group = (
    top_rows
    .groupBy("ticket_id")
    .agg(
        F.sum(F.when(F.col("event_type").isin("assigned", "resolved"), F.col("rows_in_ticket_csat_event_type"))).alias("rows_in_ticket_csat_event_type_event_type_in_ass"),
        F.round(F.avg("ticket_csat_change"), 2).alias("avg_ticket_csat_change"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("ticket_csat_event_type_min_ticket_csat"), 2).alias("stddev_ticket_csat_event_type_min_ticket_csat"),
        F.round(F.sum("ticket_closed_at_rank") / F.nullif(F.sum("ticket_closed_at_seq"), F.lit(0)), 3).alias("ticket_closed_at_rank_to_ticket_closed_at_seq_ra"),
        F.sum(F.when(F.col("actor").isNotNull(), 1).otherwise(0)).alias("n_actor_is_not_null"),
    )
)

result = (
    per_group
    .select(
        "ticket_id",
        "rows_in_ticket_csat_event_type_event_type_in_ass",
        "avg_ticket_csat_change",
        "row_count",
        "stddev_ticket_csat_event_type_min_ticket_csat",
        "ticket_closed_at_rank_to_ticket_closed_at_seq_ra",
        "n_actor_is_not_null",
    )
    .orderBy("row_count", F.col("ticket_id").desc())
)
