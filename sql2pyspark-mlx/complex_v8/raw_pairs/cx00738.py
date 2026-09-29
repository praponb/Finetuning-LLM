from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_closed_at_to_date = Window.partitionBy("priority", F.to_date("closed_at"))
w_priority_by_closed_at = Window.partitionBy("priority").orderBy("closed_at", "id")
w_closed_at_month_opened_at_mo = Window.partitionBy(F.month("closed_at"), F.date_trunc("month", "opened_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_priority_by_opened = Window.partitionBy("agent_id", "priority").orderBy(F.col("opened_at").desc())
w_opened_at_year_agent_id_by_r = Window.partitionBy(F.year("opened_at"), "agent_id").orderBy("requester_id", "id")
w_agent_id_priority_by_opened_2 = Window.partitionBy("agent_id", "priority").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_year_agent_id_by_r_2 = Window.partitionBy(F.year("opened_at"), "agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority_closed_at_to_date), 2).alias("requester_id_vs_priority_closed_at_to_date_avg"),
        F.row_number().over(w_priority_by_closed_at).alias("rn"),
        F.nth_value("requester_id", 2).over(w_closed_at_month_opened_at_mo).alias("second_requester_id"),
        F.dense_rank().over(w_agent_id_priority_by_opened).alias("opened_at_dense_rank"),
        F.ntile(4).over(w_opened_at_year_agent_id_by_r).alias("requester_id_quartile"),
        F.ntile(10).over(w_opened_at_year_agent_id_by_r).alias("requester_id_decile"),
        F.count(F.lit(1)).over(w_agent_id_priority_by_opened_2).alias("running_count"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_priority_closed_at_to_date), F.lit(0)), 4).alias("requester_id_share_of_priority_closed_at_to_date"),
        F.nth_value("closed_at", 2).over(w_opened_at_year_agent_id_by_r_2).alias("second_closed_at"),
    )
)

first_rows = (
    sequenced
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    first_rows
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "requester_id_quartile",
        "requester_id_decile",
        "requester_id_share_of_priority_closed_at_to_date",
        "second_closed_at",
        F.abs(F.col("running_count") - 7).alias("running_count_distance"),
    )
    .orderBy(F.col("running_count_distance").desc_nulls_first(), "second_closed_at")
)
