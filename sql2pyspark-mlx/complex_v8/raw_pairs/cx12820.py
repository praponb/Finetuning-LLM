from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_all_by_n_priority_p1 = Window.orderBy("n_priority_p1", "agent_id", "priority")
w_all_by_last_opened_at_week_f = Window.orderBy(F.col("last_opened_at_week").desc(), "agent_id", "priority").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_priority_p1_moving = Window.orderBy("n_priority_p1", "agent_id", "priority").rowsBetween(-6, Window.currentRow)

base_tickets = (
    df_tickets
    .filter(F.col("priority").isin("P4", "P3", "P2"))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
    )
)

summary = (
    base_tickets
    .filter(F.array_contains(F.col("tags"), "bug"))
    .groupBy("agent_id", "priority")
    .agg(
        F.sum("least_csat_requester_id").alias("total_least_csat_requester_id"),
        F.count("*").alias("row_count"),
        F.max("opened_at_week").alias("last_opened_at_week"),
        F.count(F.when(F.col("priority") == "P1", 1)).alias("n_priority_p1"),
        F.round(F.avg("least_csat_requester_id"), 2).alias("avg_least_csat_requester_id"),
        F.min("least_csat_requester_id").alias("min_least_csat_requester_id"),
    )
)

with_windows = (
    summary
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.ntile(3).over(w_all_by_n_priority_p1).alias("n_priority_p1_tercile"),
        F.nth_value("row_count", 2).over(w_all_by_last_opened_at_week_f).alias("second_row_count"),
        F.round(F.col("avg_least_csat_requester_id") - F.avg("avg_least_csat_requester_id").over(w_priority), 2).alias("avg_least_csat_requester_id_vs_priority_avg"),
        F.round(F.avg("min_least_csat_requester_id").over(w_all_by_n_priority_p1_moving), 2).alias("min_least_csat_requester_id_avg_7"),
    )
)

result = (
    with_windows
    .groupBy("n_priority_p1_tercile")
    .pivot("priority", ["P4", "P3"])
    .agg(F.sum("distinct_agent_id_in_priority"))
    .orderBy("n_priority_p1_tercile")
)
