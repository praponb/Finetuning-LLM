from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_csat = Window.partitionBy("priority", "csat")
w_channel_csat_by_csat_prior_6 = Window.partitionBy("channel", "csat").orderBy("csat", "id").rowsBetween(-6, -1)
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_priority_by_csat_running = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_moving_1 = Window.orderBy("requester_id", "id").rowsBetween(-1, Window.currentRow)
w_priority_by_csat_ties = Window.partitionBy("priority").orderBy(F.col("csat").desc())
w_priority_by_csat = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id")

recent = df_tickets.filter((F.col("agent_id") >= 7) & (F.col("priority").isin("P1", "P3", "P4")))

sequenced = (
    recent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.round(F.col("csat") - F.avg("csat").over(w_priority_csat), 2).alias("csat_vs_priority_csat_avg"),
        F.sum("requester_id").over(w_channel_csat_by_csat_prior_6).alias("requester_id_sum_6_prior"),
        F.max("csat").over(w_priority_csat).alias("priority_csat_max_csat"),
        F.round(F.cume_dist().over(w_all_by_requester_id), 3).alias("requester_id_cume_dist"),
        F.count(F.lit(1)).over(w_priority_by_csat_running).alias("running_count"),
        F.count(F.lit(1)).over(w_priority_csat).alias("rows_in_priority_csat"),
        F.sum("csat").over(w_priority_csat).alias("priority_csat_sum_csat"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority_csat), 2).alias("requester_id_vs_priority_csat_avg"),
        F.round(F.avg("csat").over(w_all_by_requester_id_moving_1), 2).alias("csat_avg_2"),
        F.rank().over(w_priority_by_csat_ties).alias("csat_rank"),
        F.last("csat", ignorenulls=True).over(w_priority_by_csat_running).alias("last_known_csat"),
        F.ntile(10).over(w_priority_by_csat).alias("csat_decile"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_requester_id)).alias("requester_id_change"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        "csat_vs_priority_csat_avg",
        "requester_id_sum_6_prior",
        "priority_csat_max_csat",
        "rows_in_priority_csat",
        "priority_csat_sum_csat",
        "requester_id_vs_priority_csat_avg",
        "csat_avg_2",
        "csat_rank",
        "last_known_csat",
        "requester_id_change",
    )
    .orderBy("requester_id_change")
)
