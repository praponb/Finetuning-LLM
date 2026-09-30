from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_closed_at_pl = Window.partitionBy(F.date_trunc("month", "closed_at"), F.date_trunc("month", "closed_at_plus_30m")).orderBy("closed_at_plus_30m", "id")
w_closed_at_year_agent_id = Window.partitionBy(F.year("closed_at"), "agent_id")
w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc_nulls_first())
w_all_by_requester_id = Window.orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_opened_at_month = Window.partitionBy(F.date_trunc("month", "opened_at"))
w_opened_at_month_by_closed_at = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy("closed_at_plus_30m")
w_opened_at_month_by_closed_at_2 = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy("closed_at_plus_30m", "id")
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_all_by_requester_id_running = Window.orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_ties = Window.orderBy("requester_id")

filtered = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        (F.col("closed_at") + F.expr("INTERVAL 30 MINUTES")).alias("closed_at_plus_30m"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.round(F.percent_rank().over(w_closed_at_month_closed_at_pl), 3).alias("closed_at_plus_30m_percent_rank"),
        F.dense_rank().over(w_all_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.row_number().over(w_all_by_requester_id).alias("requester_id_seq"),
        F.count(F.lit(1)).over(w_opened_at_month).alias("rows_in_opened_at_month"),
        F.round(F.cume_dist().over(w_opened_at_month_by_closed_at), 3).alias("closed_at_plus_30m_cume_dist"),
        F.round(F.cume_dist().over(w_opened_at_month_by_closed_at_2), 3).alias("closed_at_plus_30m_cume_dist_2"),
        F.ntile(5).over(w_all_by_closed_at).alias("closed_at_quintile"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_year_agent_id), F.lit(0)), 4).alias("requester_id_share_of_closed_at_year_agent_id"),
        F.round(F.avg("requester_id").over(w_closed_at_year_agent_id), 2).alias("closed_at_year_agent_id_avg_requester_id"),
        F.sum("requester_id").over(w_closed_at_year_agent_id).alias("closed_at_year_agent_id_sum_requester_id"),
        F.first("closed_at", ignorenulls=True).over(w_all_by_requester_id_running).alias("first_known_closed_at"),
        F.round((F.unix_timestamp("closed_at_plus_30m") - F.unix_timestamp(F.lag("closed_at_plus_30m").over(w_all_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at_plus_30m"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_requester_id))) / 60, 1).alias("minutes_since_prev_closed_at"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_closed_at)).alias("requester_id_change"),
        F.round(F.cume_dist().over(w_all_by_requester_id_ties), 3).alias("requester_id_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "closed_at",
        "closed_at_plus_30m",
        "closed_at_plus_30m_percent_rank",
        "closed_at_dense_rank",
        "requester_id_seq",
        "closed_at_plus_30m_cume_dist",
        "closed_at_plus_30m_cume_dist_2",
        "closed_at_quintile",
        "closed_at_year_agent_id_avg_requester_id",
        "first_known_closed_at",
        "minutes_since_prev_closed_at",
        "requester_id_cume_dist",
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
    )
    .orderBy(
        F.col("closed_at_quintile").desc_nulls_first(),
        "closed_at_plus_30m_percent_rank",
        "id",
    )
    .limit(10)
)
