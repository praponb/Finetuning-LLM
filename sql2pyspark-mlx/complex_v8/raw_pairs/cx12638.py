from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_csat_prior_1 = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_csat_closed_at_month = Window.partitionBy("csat", F.month("closed_at"))

scoped = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.sum("requester_id").over(w_agent_id_by_csat_prior_1).alias("requester_id_sum_1_prior"),
        F.sum("csat").over(w_csat_closed_at_month).alias("csat_closed_at_month_sum_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_closed_at_month), 2).alias("csat_vs_csat_closed_at_month_avg"),
    )
)

derived = (
    with_windows
    .filter(F.col("requester_id_sum_1_prior") >= 12)
    .select(
        "id",
        "closed_at",
        "channel",
        "csat",
        "requester_id_sum_1_prior",
        "csat_closed_at_month_sum_csat",
        "csat_vs_csat_closed_at_month_avg",
        F.abs(F.col("csat") - 4).alias("csat_distance"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label_2"),
    )
)

without_matches = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

screened = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.hour(F.col("t.event_at")).between(10, 14)), "left_semi")
)

result = (
    screened
    .groupBy("csat", "csat_distance")
    .pivot("channel", ["email", "phone", "web", "chat"])
    .agg(F.sum("csat_vs_csat_closed_at_month_avg"))
    .orderBy("csat", "csat_distance")
)
