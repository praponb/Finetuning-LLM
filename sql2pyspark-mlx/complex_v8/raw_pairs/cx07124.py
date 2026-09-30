from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_csat_actor_by_ticket = Window.partitionBy("ticket_csat", "actor").orderBy("ticket_opened_at", "id").rowsBetween(-1, -1)
w_ticket_id_ticket_opened_at_t = Window.partitionBy("ticket_id", F.to_date("ticket_opened_at")).orderBy(F.col("event_at").desc(), "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_event_at_month = Window.partitionBy(F.month("event_at"))
w_event_type_by_ticket_opened = Window.partitionBy("event_type").orderBy("ticket_opened_at", "id")
w_ticket_csat_actor_by_ticket_2 = Window.partitionBy("ticket_csat", "actor").orderBy("ticket_opened_at", "id")
w_ticket_csat_by_ticket_csat = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_csat").desc_nulls_first(), "id")
w_all_by_event_at_moving_6 = Window.orderBy("event_at", "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_seq_no = Window.partitionBy("ticket_id", "seq_no")
w_ticket_csat_by_ticket_csat_s = Window.partitionBy("ticket_csat").orderBy("ticket_csat_share_of_ticket_id", "id")
w_ticket_agent_id_distinct_act = Window.partitionBy("ticket_agent_id", "distinct_actor_in_event_at_month").orderBy("ticket_csat_share_of_ticket_id", "id").rowsBetween(-2, Window.currentRow)
w_actor_by_ticket_csat_running = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_events = df_ticket_events.filter(F.col("event_type") != "reopened")

combined_details_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_windows = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_csat",
        "ticket_agent_id",
        "ticket_opened_at",
        F.sum("ticket_csat").over(w_ticket_csat_actor_by_ticket).alias("ticket_csat_sum_1_prior"),
        F.round((F.unix_timestamp("ticket_opened_at") - F.unix_timestamp(F.lag("ticket_opened_at").over(w_ticket_id_ticket_opened_at_t))) / 60, 1).alias("minutes_since_prev_ticket_opened_at"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_id), 2).alias("ticket_csat_vs_ticket_id_avg"),
        F.size(F.collect_set("actor").over(w_event_at_month)).alias("distinct_actor_in_event_at_month"),
        F.row_number().over(w_event_type_by_ticket_opened).alias("seq_no"),
        F.lag("ticket_opened_at").over(w_ticket_csat_actor_by_ticket_2).alias("prev_ticket_opened_at"),
        F.round(F.percent_rank().over(w_ticket_csat_by_ticket_csat), 3).alias("ticket_csat_percent_rank"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_event_at_month), 2).alias("ticket_csat_vs_event_at_month_avg"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_event_at_month), F.lit(0)), 4).alias("ticket_csat_share_of_event_at_month"),
        F.row_number().over(w_ticket_csat_actor_by_ticket_2).alias("seq_no_3"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_id), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_id"),
        F.max("ticket_csat").over(w_all_by_event_at_moving_6).alias("ticket_csat_max_7"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_csat",
        "ticket_opened_at",
        "ticket_csat_sum_1_prior",
        "ticket_csat_vs_ticket_id_avg",
        "prev_ticket_opened_at",
        "ticket_csat_vs_event_at_month_avg",
        "ticket_csat_share_of_event_at_month",
        "seq_no_3",
        "ticket_csat_share_of_ticket_id",
        "ticket_csat_max_7",
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_id_seq_no), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_id_seq_no"),
        F.min("seq_no").over(w_ticket_id_seq_no).alias("ticket_id_seq_no_min_seq_no"),
        F.dense_rank().over(w_ticket_csat_by_ticket_csat_s).alias("ticket_csat_share_of_ticket_id_dense_rank"),
        F.size(F.collect_set("ticket_agent_id").over(w_ticket_id_seq_no)).alias("distinct_ticket_agent_id_in_ticket_id_seq_no"),
        F.max("seq_no").over(w_ticket_id_seq_no).alias("ticket_id_seq_no_max_seq_no"),
        F.round(F.avg("ticket_csat_max_7").over(w_ticket_agent_id_distinct_act), 2).alias("ticket_csat_max_7_avg_3"),
        F.first("seq_no_3", ignorenulls=True).over(w_actor_by_ticket_csat_running).alias("first_known_seq_no_3"),
        F.lead("seq_no").over(w_ticket_csat_by_ticket_csat_s).alias("next_seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "actor",
        "ticket_csat",
        "ticket_csat_sum_1_prior",
        "prev_ticket_opened_at",
        "ticket_csat_vs_event_at_month_avg",
        "seq_no_3",
        "ticket_csat_share_of_ticket_id_dense_rank",
        "distinct_ticket_agent_id_in_ticket_id_seq_no",
        "ticket_csat_max_7_avg_3",
        "first_known_seq_no_3",
        "next_seq_no",
        F.greatest(F.col("ticket_csat_sum_1_prior"), F.col("first_known_seq_no_3")).alias("greatest_ticket_csat_sum_1_prior_first_known_seq"),
    )
    .orderBy(
        "ticket_csat_vs_event_at_month_avg",
        F.col("prev_ticket_opened_at").desc(),
    )
)
