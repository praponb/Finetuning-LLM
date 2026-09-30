from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_id_by_ticket_csat_run = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_ticket_csat_tie = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat").asc_nulls_last())
w_ticket_id_by_ticket_csat = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat").asc_nulls_last(), "id")
w_ticket_csat_agent_hired_on_m = Window.partitionBy("ticket_csat", F.month("agent_hired_on")).orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(-1, -1)
w_ticket_id_by_ticket_csat_mov = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_ticket_csat_agent_hired_on_m_2 = Window.partitionBy("ticket_csat", F.month("agent_hired_on")).orderBy(F.col("agent_hired_on").desc(), "id")
w_agent_hired_on_year = Window.partitionBy(F.year("agent_hired_on"))
w_ticket_channel_by_ticket_csa = Window.partitionBy("ticket_channel").orderBy(F.col("ticket_csat").asc_nulls_last())
w_ticket_id_by_ticket_csat_ful = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_agent_hired_on_m_3 = Window.partitionBy("ticket_csat", F.month("agent_hired_on")).orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_ticket_csat_dens = Window.partitionBy("ticket_csat", "ticket_csat_dense_rank_2")
w_ticket_csat_ticket_csat_dens_2 = Window.partitionBy("ticket_csat", "ticket_csat_dense_rank_2").orderBy(F.col("ticket_csat").desc(), "id")
w_agent_hired_on_month_ticket = Window.partitionBy(F.month("agent_hired_on"), "ticket_channel").orderBy("agent_hired_on_year_max_ticket_csat", "id")

scoped = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type_actor_label",
        "s.event_type_slug",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    with_details
    .select(
        "id",
        "event_type_slug",
        "ticket_csat",
        "ticket_channel",
        "agent_hired_on",
        F.max("ticket_csat").over(w_ticket_id).alias("ticket_id_max_ticket_csat"),
        F.sum("ticket_csat").over(w_ticket_id_by_ticket_csat_run).alias("running_ticket_csat"),
        F.dense_rank().over(w_ticket_id_by_ticket_csat_tie).alias("ticket_csat_dense_rank"),
        F.lead("agent_hired_on", 3).over(w_ticket_id_by_ticket_csat).alias("next3_agent_hired_on"),
        F.round(F.avg("ticket_csat").over(w_ticket_csat_agent_hired_on_m), 2).alias("ticket_csat_avg_1_prior"),
        F.sum("ticket_csat").over(w_ticket_id_by_ticket_csat_mov).alias("ticket_csat_sum_2"),
        F.row_number().over(w_ticket_csat_agent_hired_on_m_2).alias("seq_no"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_agent_hired_on_year), 2).alias("ticket_csat_vs_agent_hired_on_year_avg"),
        F.min("ticket_csat").over(w_agent_hired_on_year).alias("agent_hired_on_year_min_ticket_csat"),
        F.round(F.avg("ticket_csat").over(w_ticket_id), 2).alias("ticket_id_avg_ticket_csat"),
        F.dense_rank().over(w_ticket_channel_by_ticket_csa).alias("ticket_csat_dense_rank_2"),
        F.first("agent_hired_on").over(w_ticket_id_by_ticket_csat_ful).alias("first_agent_hired_on"),
        F.max("ticket_csat").over(w_agent_hired_on_year).alias("agent_hired_on_year_max_ticket_csat"),
        F.first("ticket_csat").over(w_ticket_csat_agent_hired_on_m_3).alias("first_ticket_csat"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.round(F.col("running_ticket_csat") / F.nullif(F.sum("running_ticket_csat").over(w_ticket_csat_ticket_csat_dens), F.lit(0)), 4).alias("running_ticket_csat_share_of_ticket_csat_ticket_"),
        F.round(F.col("ticket_csat_avg_1_prior") / F.nullif(F.sum("ticket_csat_avg_1_prior").over(w_ticket_csat_ticket_csat_dens), F.lit(0)), 4).alias("ticket_csat_avg_1_prior_share_of_ticket_csat_tic"),
        F.round(F.avg("seq_no").over(w_ticket_csat_ticket_csat_dens), 2).alias("ticket_csat_ticket_csat_dense_rank_2_avg_seq_no"),
        F.datediff(F.col("next3_agent_hired_on"), F.lag("next3_agent_hired_on").over(w_ticket_csat_ticket_csat_dens_2)).alias("days_since_prev_next3_agent_hired_on"),
        F.count(F.lit(1)).over(w_ticket_csat_ticket_csat_dens).alias("rows_in_ticket_csat_ticket_csat_dense_rank_2"),
        F.round(F.avg("ticket_csat").over(w_ticket_csat_ticket_csat_dens), 2).alias("ticket_csat_ticket_csat_dense_rank_2_avg_ticket_"),
        F.round(F.avg("ticket_csat_sum_2").over(w_ticket_csat_ticket_csat_dens), 2).alias("ticket_csat_ticket_csat_dense_rank_2_avg_ticket__2"),
        (F.col("ticket_csat_dense_rank") - F.lag("ticket_csat_dense_rank").over(w_agent_hired_on_month_ticket)).alias("ticket_csat_dense_rank_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_csat",
        "ticket_channel",
        "agent_hired_on",
        "ticket_id_max_ticket_csat",
        "running_ticket_csat",
        "ticket_csat_avg_1_prior",
        "ticket_csat_sum_2",
        "seq_no",
        "ticket_csat_vs_agent_hired_on_year_avg",
        "agent_hired_on_year_min_ticket_csat",
        "ticket_id_avg_ticket_csat",
        "ticket_csat_dense_rank_2",
        "first_agent_hired_on",
        "first_ticket_csat",
        "ticket_csat_avg_1_prior_share_of_ticket_csat_tic",
        "ticket_csat_ticket_csat_dense_rank_2_avg_seq_no",
        "days_since_prev_next3_agent_hired_on",
        "rows_in_ticket_csat_ticket_csat_dense_rank_2",
        "ticket_csat_ticket_csat_dense_rank_2_avg_ticket_",
        "ticket_csat_ticket_csat_dense_rank_2_avg_ticket__2",
        "ticket_csat_dense_rank_change",
        F.abs(F.col("ticket_csat_sum_2") - 9).alias("ticket_csat_sum_2_distance"),
    )
    .orderBy(
        F.col("ticket_csat_sum_2_distance").desc_nulls_first(),
        F.col("days_since_prev_next3_agent_hired_on").desc_nulls_first(),
    )
)
