from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_priority_by_requester_i = Window.partitionBy("csat", "priority").orderBy(F.col("requester_id").desc(), "id")
w_closed_at_to_date_agent_id = Window.partitionBy(F.to_date("closed_at"), "agent_id")
w_agent_id_channel_by_requeste = Window.partitionBy("agent_id", "channel").orderBy("requester_id", "id")
w_csat_priority_by_requester_i_2 = Window.partitionBy("csat", "priority").orderBy("requester_id", "id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_csat_priority_by_requester_i_3 = Window.partitionBy("csat", "priority").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_closed_at_year_by_o = Window.partitionBy("agent_id", F.year("closed_at")).orderBy(F.col("opened_at").desc())

scoped = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-12 11:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "priority_slug",
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_csat_priority_by_requester_i))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.col("csat") - F.avg("csat").over(w_closed_at_to_date_agent_id), 2).alias("csat_vs_closed_at_to_date_agent_id_avg"),
        F.count(F.lit(1)).over(w_closed_at_to_date_agent_id).alias("rows_in_closed_at_to_date_agent_id"),
        (F.col("requester_id") - F.lag("requester_id").over(w_agent_id_channel_by_requeste)).alias("requester_id_change"),
        F.lag("closed_at").over(w_csat_priority_by_requester_i_2).alias("prev_closed_at"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_csat), 2).alias("csat_vs_agent_id_csat_avg"),
        F.nth_value("requester_id", 2).over(w_csat_priority_by_requester_i_3).alias("second_requester_id"),
        F.ntile(5).over(w_agent_id_channel_by_requeste).alias("requester_id_quintile"),
        F.round(F.percent_rank().over(w_agent_id_closed_at_year_by_o), 3).alias("opened_at_percent_rank"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.nth_value("opened_at", 2).over(w_csat_priority_by_requester_i_3).alias("second_opened_at"),
        F.row_number().over(w_agent_id_channel_by_requeste).alias("rn"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_to_date_agent_id), 2).alias("requester_id_vs_closed_at_to_date_agent_id_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "priority_slug",
        "csat_vs_closed_at_to_date_agent_id_avg",
        "requester_id_change",
        "csat_vs_agent_id_csat_avg",
        "second_requester_id",
        "requester_id_quintile",
        "opened_at_percent_rank",
        "rows_in_agent_id_csat",
        "second_opened_at",
        "rn",
        "requester_id_vs_closed_at_to_date_agent_id_avg",
        F.date_trunc("month", F.col("closed_at")).alias("closed_at_month"),
    )
    .orderBy(F.col("rn").desc(), "priority_slug")
)
