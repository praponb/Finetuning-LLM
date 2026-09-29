from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_priority_by_requeste = Window.partitionBy("channel", "priority").orderBy(F.col("requester_id").desc_nulls_first(), "id")
w_opened_at_month = Window.partitionBy(F.month("opened_at"))
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_all_by_opened_at_moving_3 = Window.orderBy("opened_at", "id").rowsBetween(-3, Window.currentRow)
w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc())
w_channel_priority_by_requeste_2 = Window.partitionBy("channel", "priority").orderBy(F.col("requester_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_2 = Window.partitionBy(F.date_trunc("month", "opened_at"))
w_channel_opened_at_month_by_o = Window.partitionBy("channel", F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at").desc(), "id")
w_channel_opened_at_month_by_o_2 = Window.partitionBy("channel", F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at").desc())
w_channel_opened_at_month_by_o_3 = Window.partitionBy("channel", F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_moving_6 = Window.orderBy("requester_id", "id").rowsBetween(-6, Window.currentRow)

prepared = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")) & (F.col("closed_at") < F.lit("2025-02-26 16:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.lead("closed_at", 2).over(w_channel_priority_by_requeste).alias("next2_closed_at"),
        F.count(F.lit(1)).over(w_opened_at_month).alias("rows_in_opened_at_month"),
        F.round(F.percent_rank().over(w_all_by_closed_at), 3).alias("closed_at_percent_rank"),
        F.round(F.avg("requester_id").over(w_all_by_opened_at_moving_3), 2).alias("requester_id_avg_4"),
        F.dense_rank().over(w_all_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.sum("requester_id").over(w_opened_at_month).alias("opened_at_month_sum_requester_id"),
        F.first("closed_at").over(w_channel_priority_by_requeste_2).alias("first_closed_at"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_opened_at_month_2), 2).alias("requester_id_vs_opened_at_month_avg"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_closed_at)).alias("requester_id_change"),
    )
)

top_rows = (
    with_trends
    .filter((F.col("closed_at_dense_rank") == 1) & (F.col("requester_id_change") <= 12))
    .drop("closed_at_dense_rank")
)

windowed = (
    top_rows
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        "priority_prefix",
        "rows_in_opened_at_month",
        "requester_id_avg_4",
        F.lead("opened_at").over(w_channel_opened_at_month_by_o).alias("next_opened_at"),
        F.dense_rank().over(w_channel_opened_at_month_by_o_2).alias("opened_at_dense_rank"),
        F.dense_rank().over(w_channel_opened_at_month_by_o).alias("opened_at_dense_rank_2"),
        F.last("closed_at_percent_rank", ignorenulls=True).over(w_channel_opened_at_month_by_o_3).alias("last_known_closed_at_percent_rank"),
        F.first("closed_at", ignorenulls=True).over(w_channel_opened_at_month_by_o_3).alias("first_known_closed_at"),
        F.max("requester_id_change").over(w_all_by_requester_id_moving_6).alias("requester_id_change_max_7"),
    )
)

result = (
    windowed
    .filter(F.col("next_opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .select(
        "id",
        "channel",
        "rows_in_opened_at_month",
        "requester_id_avg_4",
        "next_opened_at",
        "opened_at_dense_rank",
        "opened_at_dense_rank_2",
        "last_known_closed_at_percent_rank",
        "first_known_closed_at",
        F.round((F.unix_timestamp(F.col("next_opened_at")) - F.unix_timestamp(F.col("first_known_closed_at"))) / 60, 1).alias("minutes_first_known_closed_at_to_next_opened_at"),
        F.date_trunc("quarter", F.col("next_opened_at")).alias("next_opened_at_quarter"),
    )
    .orderBy(F.col("id").desc(), F.col("next_opened_at_quarter").desc())
    .limit(10)
)
