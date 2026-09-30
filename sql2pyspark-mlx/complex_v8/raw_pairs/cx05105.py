from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_closed_at_year_by_csat = Window.partitionBy(F.year("closed_at")).orderBy(F.col("csat").desc(), "id")
w_csat_by_requester_id_ties = Window.partitionBy("csat").orderBy(F.col("requester_id").desc())
w_csat_by_requester_id_full = Window.partitionBy("csat").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_channel_csat_by_requester_id = Window.partitionBy("channel", "csat").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_csat_by_requester_id_2 = Window.partitionBy("channel", "csat").orderBy(F.col("requester_id").desc(), "id")
w_all_by_closed_at_full = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_first_closed_at_month_csat = Window.partitionBy(F.month("first_closed_at"), "csat")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_channel_rn_by_closed_at_runn = Window.partitionBy("channel", "rn").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")

prepared = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) & (F.col("csat") >= 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd HH:00").alias("opened_at_label"),
    )
)

ranked = (
    prepared
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "opened_at_label",
        F.round(F.avg("csat").over(w_channel), 2).alias("channel_avg_csat"),
        F.lead("requester_id").over(w_closed_at_year_by_csat).alias("next_requester_id"),
        F.round(F.cume_dist().over(w_csat_by_requester_id_ties), 3).alias("requester_id_cume_dist"),
        F.first("closed_at").over(w_csat_by_requester_id_full).alias("first_closed_at"),
        F.dense_rank().over(w_csat_by_requester_id_ties).alias("requester_id_dense_rank"),
        F.min("requester_id").over(w_agent_id_priority).alias("agent_id_priority_min_requester_id"),
        F.nth_value("csat", 2).over(w_channel_csat_by_requester_id).alias("second_csat"),
        F.row_number().over(w_channel_csat_by_requester_id_2).alias("rn"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "next_requester_id",
        "requester_id_cume_dist",
        "first_closed_at",
        "second_csat",
        F.nth_value("first_closed_at", 2).over(w_all_by_closed_at_full).alias("second_first_closed_at"),
        F.lag("csat", 3).over(w_all_by_closed_at).alias("prev3_csat"),
        F.min("second_csat").over(w_first_closed_at_month_csat).alias("first_closed_at_month_csat_min_second_csat"),
        F.count(F.lit(1)).over(w_first_closed_at_month_csat).alias("rows_in_first_closed_at_month_csat"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.sum("requester_id_cume_dist").over(w_channel_rn_by_closed_at_runn).alias("running_requester_id_cume_dist"),
        F.round(F.col("requester_id_dense_rank") / F.nullif(F.sum("requester_id_dense_rank").over(w_first_closed_at_month_csat), F.lit(0)), 4).alias("requester_id_dense_rank_share_of_first_closed_at"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
    )
)

result = (
    sequenced
    .filter(F.col("requester_id_cume_dist").between(0.1, 0.8))
    .select(
        "id",
        "closed_at",
        "priority",
        "channel",
        "next_requester_id",
        "requester_id_cume_dist",
        "first_closed_at",
        "second_csat",
        "second_first_closed_at",
        "prev3_csat",
        "rows_in_first_closed_at_month_csat",
        "closed_at_percent_rank",
        "rows_in_agent_id",
    )
)
