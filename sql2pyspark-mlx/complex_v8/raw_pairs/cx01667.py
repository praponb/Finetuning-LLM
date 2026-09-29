from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_closed_at_month_by_c = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy("csat", "id").rowsBetween(-1, Window.currentRow)
w_channel = Window.partitionBy("channel")
w_channel_closed_at_month_by_c_2 = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy("csat", "id").rowsBetween(-3, -1)
w_channel_csat = Window.partitionBy("channel", "csat")
w_opened_at_to_date_csat_by_cl = Window.partitionBy(F.to_date("opened_at"), "csat").orderBy("closed_at", "id").rowsBetween(-6, Window.currentRow)
w_agent_id_csat_by_opened_at_r = Window.partitionBy("agent_id", "csat").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.col("csat") <= 3)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

ranked = (
    recent_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.max("csat").over(w_channel_closed_at_month_by_c).alias("csat_max_2"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel), F.lit(0)), 4).alias("csat_share_of_channel"),
        F.max("requester_id").over(w_channel_closed_at_month_by_c_2).alias("requester_id_max_3_prior"),
        F.round(F.col("csat") - F.avg("csat").over(w_channel), 2).alias("csat_vs_channel_avg"),
        F.min("csat").over(w_channel_csat).alias("channel_csat_min_csat"),
        F.round(F.avg("csat").over(w_opened_at_to_date_csat_by_cl), 2).alias("csat_avg_7"),
        F.min("requester_id").over(w_agent_id_csat_by_opened_at_r).alias("min_requester_id_so_far"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel_csat), F.lit(0)), 4).alias("csat_share_of_channel_csat"),
    )
)

result = (
    ranked
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "csat_max_2",
        "csat_share_of_channel",
        "requester_id_max_3_prior",
        "csat_avg_7",
        "min_requester_id_so_far",
        "distinct_agent_id_in_channel",
        "csat_share_of_channel_csat",
    )
    .orderBy(
        F.col("requester_id_max_3_prior").asc_nulls_last(),
        "distinct_agent_id_in_channel",
    )
)
