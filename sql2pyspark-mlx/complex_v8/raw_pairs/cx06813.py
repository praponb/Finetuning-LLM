from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_closed_at_full = Window.partitionBy("channel").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_channel_by_closed_at = Window.partitionBy("channel").orderBy(F.col("closed_at").desc(), "id")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_all_by_closed_at_2 = Window.orderBy("closed_at", "id")
w_channel_by_closed_at_running = Window.partitionBy("channel").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").asc_nulls_last())
w_channel_by_closed_at_2 = Window.partitionBy("channel").orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_channel_by_closed_at_full_2 = Window.partitionBy("channel").orderBy(F.col("closed_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tickets
    .filter((F.col("csat") >= 2) & (F.hour(F.col("opened_at")).between(10, 16)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.first("closed_at").over(w_channel_by_closed_at_full).alias("first_closed_at"),
        F.lag("opened_at").over(w_all_by_closed_at).alias("prev_opened_at"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_channel_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.dense_rank().over(w_all_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.lag("closed_at").over(w_channel_by_closed_at).alias("prev_closed_at"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_closed_at_2))) / 60, 1).alias("minutes_since_prev_closed_at_2"),
        F.last("opened_at", ignorenulls=True).over(w_channel_by_closed_at_running).alias("last_known_opened_at"),
        F.dense_rank().over(w_all_by_opened_at_ties).alias("opened_at_dense_rank"),
        F.round(F.percent_rank().over(w_channel_by_closed_at_2), 3).alias("closed_at_percent_rank"),
        F.last("opened_at").over(w_channel_by_closed_at_full_2).alias("last_opened_at"),
        F.dense_rank().over(w_channel_by_closed_at).alias("closed_at_dense_rank_2"),
        F.lag("opened_at", 2).over(w_all_by_closed_at).alias("prev2_opened_at"),
        F.first("opened_at").over(w_all_by_closed_at_full).alias("first_opened_at"),
    )
)

leaders = (
    ranked
    .filter(F.col("opened_at_dense_rank") == 1)
    .drop("opened_at_dense_rank")
)

result = (
    leaders
    .filter(F.month(F.col("prev2_opened_at")).isin(2, 3))
    .select(
        "id",
        "opened_at",
        "closed_at",
        "first_closed_at",
        "prev_opened_at",
        "minutes_since_prev_closed_at",
        "prev_closed_at",
        "minutes_since_prev_closed_at_2",
        "last_known_opened_at",
        "closed_at_percent_rank",
        "last_opened_at",
        "closed_at_dense_rank_2",
        "prev2_opened_at",
        "first_opened_at",
        (F.col("closed_at_dense_rank") - F.col("closed_at_percent_rank")).alias("closed_at_dense_rank_minus_closed_at_percent_ran"),
    )
    .orderBy(F.col("closed_at").desc(), "first_opened_at")
)
