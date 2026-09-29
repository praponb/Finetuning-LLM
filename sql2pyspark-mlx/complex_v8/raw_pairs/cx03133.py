from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_by_requester = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_csat_opened_at_hour_month = Window.partitionBy("csat", F.month("opened_at_hour"))
w_all_by_closed_at_moving_1 = Window.orderBy("closed_at", "id").rowsBetween(-1, Window.currentRow)
w_closed_at_month_by_requester_2 = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")
w_closed_at_month_by_requester_3 = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id = Window.orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_all_by_closed_at_prior_2 = Window.orderBy("closed_at", "id").rowsBetween(-2, -1)
w_csat_agent_id_by_opened_at_h = Window.partitionBy("csat", "agent_id").orderBy("opened_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_month_by_requester_4 = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("requester_id").asc_nulls_last())

cleaned = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-12 10:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        F.date_trunc("hour", F.col("opened_at")).alias("opened_at_hour"),
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.lag("opened_at_hour", 3).over(w_closed_at_month_by_requester).alias("prev3_opened_at_hour"),
        F.rank().over(w_all_by_closed_at_ties).alias("closed_at_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_csat_opened_at_hour_month), F.lit(0)), 4).alias("requester_id_share_of_csat_opened_at_hour_month"),
        F.max("csat").over(w_all_by_closed_at_moving_1).alias("csat_max_2"),
        F.max("requester_id").over(w_closed_at_month_by_requester_2).alias("requester_id_max_3"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id), F.lit(0)), 4).alias("csat_share_of_agent_id"),
        F.first("csat").over(w_closed_at_month_by_requester_3).alias("first_csat"),
        F.row_number().over(w_all_by_requester_id).alias("seq_no"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id), 2).alias("csat_vs_agent_id_avg"),
        F.round(F.avg("csat").over(w_all_by_closed_at_prior_2), 2).alias("csat_avg_2_prior"),
        F.first("requester_id", ignorenulls=True).over(w_csat_agent_id_by_opened_at_h).alias("first_known_requester_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_id"),
        F.rank().over(w_closed_at_month_by_requester_4).alias("requester_id_rank"),
    )
)

leaders = (
    windowed
    .filter((F.col("closed_at_rank") <= 5) & (F.col("csat_vs_agent_id_avg").between(-2.5, 2.5)))
    .drop("closed_at_rank")
)

result = (
    leaders
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        "opened_at_hour",
        "prev3_opened_at_hour",
        "first_csat",
        "seq_no",
        "csat_vs_agent_id_avg",
        "csat_avg_2_prior",
        "requester_id_rank",
        F.coalesce(F.col("csat_vs_agent_id_avg"), F.lit(0.0)).alias("csat_vs_agent_id_avg_filled"),
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("opened_at_hour").desc(), "id")
    .limit(5)
)
