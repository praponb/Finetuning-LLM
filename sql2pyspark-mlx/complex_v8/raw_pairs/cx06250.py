from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_to_date_closed_at = Window.partitionBy(F.to_date("opened_at"), F.to_date("closed_at_plus_7d")).orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_to_date_closed_at_2 = Window.partitionBy(F.to_date("opened_at"), F.to_date("closed_at_plus_7d")).orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat = Window.partitionBy("csat")
w_closed_at_plus_7d_month_by_o = Window.partitionBy(F.date_trunc("month", "closed_at_plus_7d")).orderBy("opened_at", "id")
w_closed_at_plus_7d_month_by_o_2 = Window.partitionBy(F.date_trunc("month", "closed_at_plus_7d")).orderBy("opened_at", "id").rowsBetween(-2, Window.currentRow)
w_opened_at_month_csat = Window.partitionBy(F.month("opened_at"), "csat")
w_closed_at_plus_7d_month_by_o_3 = Window.partitionBy(F.date_trunc("month", "closed_at_plus_7d")).orderBy("opened_at")

scoped_tickets = (
    df_tickets
    .filter((F.col("priority") != "P2") & (F.col("csat") <= 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        (F.col("closed_at") + F.expr("INTERVAL 7 DAYS")).alias("closed_at_plus_7d"),
    )
)

with_windows = (
    scoped_tickets
    .select(
        "*",
        F.first("requester_id", ignorenulls=True).over(w_opened_at_to_date_closed_at).alias("first_known_requester_id"),
        F.sum("csat").over(w_opened_at_to_date_closed_at).alias("running_csat"),
        F.nth_value("requester_id", 2).over(w_opened_at_to_date_closed_at_2).alias("second_requester_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_csat), F.lit(0)), 4).alias("requester_id_share_of_csat"),
        F.round(F.avg("requester_id").over(w_csat), 2).alias("csat_avg_requester_id"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.lead("requester_id").over(w_closed_at_plus_7d_month_by_o).alias("next_requester_id"),
        F.round(F.avg("csat_avg_requester_id").over(w_closed_at_plus_7d_month_by_o_2), 2).alias("csat_avg_requester_id_avg_3"),
        F.sum("csat").over(w_opened_at_month_csat).alias("opened_at_month_csat_sum_csat"),
        F.lead("second_requester_id", 3, 0).over(w_closed_at_plus_7d_month_by_o).alias("next3_second_requester_id"),
        F.round(F.cume_dist().over(w_closed_at_plus_7d_month_by_o_3), 3).alias("opened_at_cume_dist"),
    )
)

result = (
    with_trends
    .select(
        "id",
        F.col("csat_avg_requester_id").alias("m_csat_avg_requester_id"),
        F.col("csat_avg_requester_id_avg_3").alias("csat_avg_requester_id_avg_3_v"),
        F.col("opened_at_cume_dist").alias("opened_at_cume_dist_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_csat_avg_requester_id", "csat_avg_requester_id_avg_3_v", "opened_at_cume_dist_v"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
