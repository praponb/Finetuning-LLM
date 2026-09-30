from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_csat = Window.partitionBy("priority", "csat")
w_csat = Window.partitionBy("csat")
w_all_by_closed_at_running = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter((F.col("requester_id") > 7) & (F.col("csat") >= 1))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
)

sequenced = (
    filtered_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.row_number().over(w_all_by_closed_at).alias("rn"),
        F.first("opened_at", ignorenulls=True).over(w_all_by_opened_at_running).alias("first_known_opened_at"),
        F.max("csat").over(w_priority_csat).alias("priority_csat_max_csat"),
        F.sum("csat").over(w_all_by_opened_at_running).alias("running_csat"),
        F.count(F.lit(1)).over(w_priority_csat).alias("rows_in_priority_csat"),
        F.size(F.collect_set("channel").over(w_priority_csat)).alias("distinct_channel_in_priority_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
        F.sum("csat").over(w_all_by_closed_at_running).alias("running_csat_2"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat), 2).alias("csat_vs_csat_avg"),
        F.ntile(10).over(w_all_by_closed_at).alias("closed_at_decile"),
    )
)

result = (
    sequenced
    .filter(F.col("rows_in_priority_csat") >= 6)
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "rows_in_priority_csat",
        "distinct_channel_in_priority_csat",
        "csat_share_of_csat",
        "running_csat_2",
        "csat_vs_csat_avg",
        "closed_at_decile",
    )
    .orderBy(F.col("running_csat_2").asc(), F.col("priority").asc_nulls_last(), "id")
    .limit(10)
)
