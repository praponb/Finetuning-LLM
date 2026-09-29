from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_at_year_ticket_id = Window.partitionBy(F.year("event_at"), "ticket_id")
w_actor_event_at_to_date_by_ti = Window.partitionBy("actor", F.to_date("event_at")).orderBy("ticket_csat")
w_ticket_csat = Window.partitionBy("ticket_csat")
w_actor_event_at_to_date_by_ti_2 = Window.partitionBy("actor", F.to_date("event_at")).orderBy("ticket_csat", "id").rowsBetween(-1, -1)
w_actor = Window.partitionBy("actor")
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_seq_running = Window.orderBy("event_at_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_seq_by_event_at_seq = Window.partitionBy("event_at_seq").orderBy(F.col("event_at_seq").desc(), "id")
w_event_type_ticket_csat_rank = Window.partitionBy("event_type", "ticket_csat_rank")
w_event_at_seq_by_event_at_seq_2 = Window.partitionBy("event_at_seq").orderBy(F.col("event_at_seq").desc())
w_event_type_event_at_seq_by_t = Window.partitionBy("event_type", "event_at_seq").orderBy("ticket_csat_sum_1_prior", "id").rowsBetween(-2, -1)

cleaned_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 3)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_details = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "event_type",
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.sum("ticket_csat").over(w_event_at_year_ticket_id).alias("event_at_year_ticket_id_sum_ticket_csat"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_event_at_year_ticket_id), 2).alias("ticket_csat_vs_event_at_year_ticket_id_avg"),
        F.rank().over(w_actor_event_at_to_date_by_ti).alias("ticket_csat_rank"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_csat), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_csat"),
        F.max("ticket_csat").over(w_event_at_year_ticket_id).alias("event_at_year_ticket_id_max_ticket_csat"),
        F.sum("ticket_csat").over(w_actor_event_at_to_date_by_ti_2).alias("ticket_csat_sum_1_prior"),
        F.min("ticket_csat").over(w_actor).alias("actor_min_ticket_csat"),
        F.lead("event_at", 2).over(w_all_by_event_at_2).alias("next2_event_at"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "event_at_year_ticket_id_sum_ticket_csat",
        "ticket_csat_vs_event_at_year_ticket_id_avg",
        "ticket_csat_rank",
        "event_at_year_ticket_id_max_ticket_csat",
        "actor_min_ticket_csat",
        "next2_event_at",
        F.max("event_at_seq").over(w_all_by_event_at_seq_running).alias("max_event_at_seq_so_far"),
        F.dense_rank().over(w_event_at_seq_by_event_at_seq).alias("event_at_seq_dense_rank"),
        F.round(F.avg("ticket_csat_rank").over(w_event_type_ticket_csat_rank), 2).alias("event_type_ticket_csat_rank_avg_ticket_csat_rank"),
        F.dense_rank().over(w_event_at_seq_by_event_at_seq_2).alias("event_at_seq_dense_rank_2"),
        F.sum("actor_min_ticket_csat").over(w_event_type_event_at_seq_by_t).alias("actor_min_ticket_csat_sum_2_prior"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "event_at_year_ticket_id_sum_ticket_csat",
        "ticket_csat_vs_event_at_year_ticket_id_avg",
        "event_at_year_ticket_id_max_ticket_csat",
        "actor_min_ticket_csat",
        "next2_event_at",
        "event_at_seq_dense_rank",
        "event_type_ticket_csat_rank_avg_ticket_csat_rank",
        F.round(F.col("max_event_at_seq_so_far") / F.nullif(F.col("actor_min_ticket_csat"), F.lit(0)), 3).alias("max_event_at_seq_so_far_per_actor_min_ticket_csa"),
        F.month(F.col("next2_event_at")).alias("next2_event_at_month"),
    )
    .orderBy(F.col("next2_event_at").asc())
)
