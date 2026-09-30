from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_closed_at_r = Window.partitionBy("agent_id", "csat").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat = Window.partitionBy("csat")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_csat_channel_by_csat_full = Window.partitionBy("csat", "channel").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_year_agent_id_by_c = Window.partitionBy(F.year("closed_at"), "agent_id").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_closed_at_year_agent_id_by_c_2 = Window.partitionBy(F.year("closed_at"), "agent_id").orderBy(F.col("csat").asc_nulls_last(), "id")
w_agent_id_by_csat_ties = Window.partitionBy("agent_id").orderBy("csat")
w_all_by_distinct_channel_in_a = Window.orderBy(F.col("distinct_channel_in_agent_id_csat").desc(), "id")
w_csat_change_distinct_channel = Window.partitionBy("csat_change", "distinct_channel_in_agent_id_csat")
w_csat_change_by_distinct_chan = Window.partitionBy("csat_change").orderBy(F.col("distinct_channel_in_agent_id_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat_share_of_csat = Window.orderBy(F.col("csat_share_of_csat").desc(), "id")
w_distinct_channel_in_agent_id = Window.partitionBy("distinct_channel_in_agent_id_csat", "agent_id").orderBy("csat_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_rank = Window.partitionBy("csat_rank")

valid_tickets = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(1, 2)) & (F.col("channel").isin("email", "web")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
    )
)

with_windows = (
    valid_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        F.count(F.lit(1)).over(w_agent_id_csat_by_closed_at_r).alias("running_count"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.first("closed_at").over(w_csat_channel_by_csat_full).alias("first_closed_at"),
        F.round(F.avg("csat").over(w_closed_at_year_agent_id_by_c), 2).alias("csat_avg_2"),
        F.size(F.collect_set("channel").over(w_agent_id_csat)).alias("distinct_channel_in_agent_id_csat"),
        (F.col("csat") - F.lag("csat").over(w_closed_at_year_agent_id_by_c_2)).alias("csat_change"),
        F.rank().over(w_agent_id_by_csat_ties).alias("csat_rank"),
    )
)

top_rows = (
    with_windows
    .filter((F.col("csat_rank") <= 5) & (F.col("distinct_channel_in_agent_id_csat") >= 3))
)

metrics = (
    top_rows
    .select(
        "id",
        "agent_id",
        "csat_share_of_csat",
        "first_closed_at",
        "csat_avg_2",
        "distinct_channel_in_agent_id_csat",
        "csat_change",
        F.lag("csat_change").over(w_all_by_distinct_channel_in_a).alias("prev_csat_change"),
        F.size(F.collect_set("agent_id").over(w_csat_change_distinct_channel)).alias("distinct_agent_id_in_csat_change_distinct_channe"),
        F.sum("csat_avg_2").over(w_csat_change_by_distinct_chan).alias("running_csat_avg_2"),
        F.ntile(3).over(w_all_by_csat_share_of_csat).alias("csat_share_of_csat_tercile"),
        F.count(F.lit(1)).over(w_csat_change_distinct_channel).alias("rows_in_csat_change_distinct_channel_in_agent_id"),
        F.last("csat_change").over(w_distinct_channel_in_agent_id).alias("last_csat_change"),
        F.round(F.col("running_count") / F.nullif(F.sum("running_count").over(w_csat_rank), F.lit(0)), 4).alias("running_count_share_of_csat_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "csat_share_of_csat",
        "csat_avg_2",
        "distinct_channel_in_agent_id_csat",
        "distinct_agent_id_in_csat_change_distinct_channe",
        "rows_in_csat_change_distinct_channel_in_agent_id",
        "last_csat_change",
    )
    .orderBy("last_csat_change", F.col("id").desc())
)
