from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_requester_id = Window.partitionBy("csat").orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy(F.col("csat").desc(), "id")
w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_opened_at_month = Window.partitionBy("agent_id", F.month("opened_at"))
w_agent_id_by_csat_full = Window.partitionBy("agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_opened_at_running = Window.partitionBy("csat").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_csat_by_requester_i_2 = Window.partitionBy("agent_id", "csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .filter((F.col("agent_id") >= 9) & (F.col("channel") == "chat"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
    )
)

windowed = (
    valid
    .select(
        "*",
        F.ntile(4).over(w_csat_by_requester_id).alias("requester_id_quartile"),
        F.lead("csat").over(w_agent_id_by_csat).alias("next_csat"),
        F.count(F.lit(1)).over(w_csat_agent_id).alias("rows_in_csat_agent_id"),
        F.last("csat").over(w_agent_id_csat_by_requester_i).alias("last_csat"),
        F.first("requester_id").over(w_agent_id_by_csat_full).alias("first_requester_id"),
        F.round(F.cume_dist().over(w_csat_by_requester_id), 3).alias("requester_id_cume_dist"),
        F.sum("requester_id").over(w_csat_by_opened_at_running).alias("running_requester_id"),
        F.nth_value("csat", 2).over(w_agent_id_by_csat_full).alias("second_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_agent_id), 2).alias("csat_vs_csat_agent_id_avg"),
        F.min("csat").over(w_agent_id_csat_by_requester_i_2).alias("min_csat_so_far"),
        F.count(F.lit(1)).over(w_agent_id_opened_at_month).alias("rows_in_agent_id_opened_at_month"),
        F.first("requester_id").over(w_agent_id_csat_by_requester_i).alias("first_requester_id_2"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.date_trunc("day", F.col("opened_at")).alias("opened_at_day"),
        F.when(F.col("next_csat") >= 5, "HIGH").when(F.col("next_csat") >= 3, "MEDIUM").otherwise("LOW").alias("next_csat_tier"),
    )
)

result = (
    scored
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "rows_in_csat_agent_id",
        "last_csat",
        "running_requester_id",
        "second_csat",
        "csat_vs_csat_agent_id_avg",
        "rows_in_agent_id_opened_at_month",
        "first_requester_id_2",
        "opened_at_day",
        "next_csat_tier",
        F.round(F.col("requester_id_cume_dist") * 1.07, 2).alias("requester_id_cume_dist_adj"),
    )
    .orderBy(F.col("rows_in_agent_id_opened_at_month").asc_nulls_last())
)
