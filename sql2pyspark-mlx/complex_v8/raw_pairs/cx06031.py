from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_csat = Window.partitionBy("agent_id", "csat").orderBy("csat", "id")
w_closed_at_month_opened_at_mo = Window.partitionBy(F.month("closed_at"), F.date_trunc("month", "opened_at"))
w_all_by_requester_id_distance = Window.orderBy("requester_id_distance", "id")
w_agent_id_csat_by_csat_runnin = Window.partitionBy("agent_id", "csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_distance_2 = Window.orderBy("requester_id_distance")
w_all_by_requester_id_distance_3 = Window.orderBy("requester_id_distance", "id").rowsBetween(-1, Window.currentRow)
w_all_by_requester_id_distance_4 = Window.orderBy("requester_id_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_tickets = (
    df_tickets
    .filter((F.col("priority") == "P4") | (F.month(F.col("opened_at")).isin(1, 3)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.abs(F.col("requester_id") - 18).alias("requester_id_distance"),
    )
)

ranked = (
    valid_tickets
    .select(
        "*",
        F.lead("opened_at", 2).over(w_agent_id_csat_by_csat).alias("next2_opened_at"),
        F.min("requester_id_distance").over(w_closed_at_month_opened_at_mo).alias("closed_at_month_opened_at_month_min_requester_id"),
        F.lead("closed_at", 3).over(w_all_by_requester_id_distance).alias("next3_closed_at"),
        F.sum("requester_id_distance").over(w_agent_id_csat_by_csat_runnin).alias("running_requester_id_distance"),
        F.dense_rank().over(w_all_by_requester_id_distance_2).alias("requester_id_distance_dense_rank"),
        F.round(F.avg("csat").over(w_all_by_requester_id_distance_3), 2).alias("csat_avg_2"),
        F.last("closed_at").over(w_all_by_requester_id_distance_4).alias("last_closed_at"),
        F.count(F.lit(1)).over(w_closed_at_month_opened_at_mo).alias("rows_in_closed_at_month_opened_at_month"),
    )
)

result = (
    ranked
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "requester_id_distance",
        "next2_opened_at",
        "running_requester_id_distance",
        "requester_id_distance_dense_rank",
        "csat_avg_2",
        "last_closed_at",
        "rows_in_closed_at_month_opened_at_month",
        F.greatest(F.col("csat"), F.col("requester_id_distance")).alias("greatest_csat_requester_id_distance"),
    )
    .orderBy(F.col("csat_avg_2").desc())
)
