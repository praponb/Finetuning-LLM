from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_agent_hired_on_month_by_agen = Window.partitionBy(F.date_trunc("month", "agent_hired_on")).orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_agent_hired_on_mont = Window.partitionBy("agent_id", F.date_trunc("month", "agent_hired_on"))
w_agent_hired_on_month_by_agen_2 = Window.partitionBy(F.date_trunc("month", "agent_hired_on")).orderBy(F.col("agent_hired_on").desc())
w_agent_hired_on_month_by_agen_3 = Window.partitionBy(F.date_trunc("month", "agent_hired_on")).orderBy(F.col("agent_hired_on").desc(), "id")
w_csat_by_requester_id = Window.partitionBy("csat").orderBy(F.col("requester_id").desc(), "id")
w_agent_hired_on_year_by_csat = Window.partitionBy(F.year("agent_hired_on")).orderBy("csat_distance", "id")
w_agent_hired_on_year_by_csat_2 = Window.partitionBy(F.year("agent_hired_on")).orderBy("csat_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_agent_hired_on_cume_d = Window.orderBy("agent_hired_on_cume_dist", "id")
w_prev_csat_distance_by_next3 = Window.partitionBy("prev_csat_distance").orderBy(F.col("next3_requester_id").desc(), "id")

recent_tickets = (
    df_tickets
    .filter(F.col("csat").between(1, 4))
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
        F.abs(F.col("csat") - 3).alias("csat_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

joined_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.csat_distance",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    joined_agent
    .select(
        "*",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_csat), F.lit(0)), 4).alias("csat_share_of_agent_id_csat"),
        F.nth_value("requester_id", 2).over(w_agent_hired_on_month_by_agen).alias("second_requester_id"),
        F.min("csat_distance").over(w_agent_id_agent_hired_on_mont).alias("agent_id_agent_hired_on_month_min_csat_distance"),
        F.dense_rank().over(w_agent_hired_on_month_by_agen_2).alias("agent_hired_on_dense_rank"),
        F.lead("requester_id", 1, 0).over(w_agent_hired_on_month_by_agen_3).alias("next_requester_id"),
        (F.col("csat") - F.lag("csat").over(w_csat_by_requester_id)).alias("csat_change"),
        F.lag("csat_distance").over(w_csat_by_requester_id).alias("prev_csat_distance"),
        F.lead("requester_id", 3).over(w_agent_hired_on_year_by_csat).alias("next3_requester_id"),
        F.rank().over(w_csat_by_requester_id).alias("requester_id_rank"),
        F.round(F.cume_dist().over(w_agent_hired_on_month_by_agen_2), 3).alias("agent_hired_on_cume_dist"),
        F.sum("requester_id").over(w_agent_hired_on_year_by_csat_2).alias("running_requester_id"),
        F.lag("agent_hired_on").over(w_agent_hired_on_month_by_agen_3).alias("prev_agent_hired_on"),
        F.round(F.avg("requester_id").over(w_agent_id_csat), 2).alias("agent_id_csat_avg_requester_id"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.round(F.percent_rank().over(w_csat_by_requester_id), 3).alias("requester_id_percent_rank"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.row_number().over(w_all_by_agent_hired_on_cume_d).alias("rn"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_agent_hired_on_cume_d))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.percent_rank().over(w_prev_csat_distance_by_next3), 3).alias("next3_requester_id_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        "csat_distance",
        "agent_name",
        "agent_tier",
        "csat_share_of_agent_id_csat",
        "second_requester_id",
        "agent_id_agent_hired_on_month_min_csat_distance",
        "agent_hired_on_dense_rank",
        "csat_change",
        "prev_csat_distance",
        "next3_requester_id",
        "agent_hired_on_cume_dist",
        "running_requester_id",
        "agent_id_csat_avg_requester_id",
        "rows_in_agent_id_csat",
        "rn",
        "next3_requester_id_percent_rank",
    )
    .orderBy("prev_csat_distance", "agent_name")
)
