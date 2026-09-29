from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy(F.col("closed_at").asc_nulls_last(), "id")
w_agent_id_by_closed_at_ties = Window.partitionBy("agent_id").orderBy("closed_at")
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_agent_id_channel_by_opened_a = Window.partitionBy("agent_id", "channel").orderBy("opened_at", "id")
w_opened_at_month_by_opened_at = Window.partitionBy(F.month("opened_at")).orderBy("opened_at")
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_opened_at_month_by_opened_at_2 = Window.partitionBy(F.month("opened_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_by_opened_at_3 = Window.partitionBy(F.month("opened_at")).orderBy("opened_at", "id")
w_agent_id_channel_by_opened_a_2 = Window.partitionBy("agent_id", "channel").orderBy("opened_at")
w_agent_id_by_closed_at_runnin = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_channel_by_opened_a_3 = Window.partitionBy("agent_id", "channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_priority_by_rn = Window.partitionBy("channel", "priority").orderBy(F.col("rn").desc_nulls_first(), "id")
w_rn = Window.partitionBy("rn")
w_priority_opened_at_seq_by_pr = Window.partitionBy("priority", "opened_at_seq").orderBy(F.col("prev_closed_at_2").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter((F.col("channel") == "phone") & (F.col("priority") == "P2"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

sequenced = (
    valid_tickets
    .select(
        "*",
        F.lag("closed_at").over(w_all_by_closed_at).alias("prev_closed_at"),
        F.round(F.cume_dist().over(w_agent_id_by_closed_at_ties), 3).alias("closed_at_cume_dist"),
        F.lag("opened_at", 3).over(w_all_by_closed_at).alias("prev3_opened_at"),
        F.row_number().over(w_agent_id_by_opened_at).alias("opened_at_seq"),
        F.lag("closed_at").over(w_agent_id_channel_by_opened_a).alias("prev_closed_at_2"),
        F.round(F.percent_rank().over(w_opened_at_month_by_opened_at), 3).alias("opened_at_percent_rank"),
        F.lead("opened_at").over(w_agent_id_by_closed_at).alias("next_opened_at"),
        F.nth_value("opened_at", 2).over(w_opened_at_month_by_opened_at_2).alias("second_opened_at"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_opened_at_month_by_opened_at_3))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.row_number().over(w_all_by_closed_at).alias("rn"),
        F.rank().over(w_agent_id_channel_by_opened_a_2).alias("opened_at_rank"),
        F.ntile(4).over(w_agent_id_by_closed_at).alias("closed_at_quartile"),
        F.first("closed_at", ignorenulls=True).over(w_agent_id_by_closed_at_runnin).alias("first_known_closed_at"),
        F.last("closed_at").over(w_agent_id_channel_by_opened_a_3).alias("last_closed_at"),
    )
)

first_rows = sequenced.filter(F.col("rn") <= 5)

with_windows = (
    first_rows
    .select(
        "*",
        (F.col("rn") - F.lag("rn").over(w_channel_priority_by_rn)).alias("rn_change"),
        F.round(F.col("opened_at_rank") / F.nullif(F.sum("opened_at_rank").over(w_rn), F.lit(0)), 4).alias("opened_at_rank_share_of_rn"),
        F.max("opened_at_rank").over(w_priority_opened_at_seq_by_pr).alias("opened_at_rank_max_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "minutes_opened_at_to_closed_at",
        "prev_closed_at",
        "closed_at_cume_dist",
        "prev3_opened_at",
        "opened_at_seq",
        "prev_closed_at_2",
        "opened_at_percent_rank",
        "next_opened_at",
        "minutes_since_prev_closed_at",
        "rn",
        "opened_at_rank",
        "last_closed_at",
        "rn_change",
        "opened_at_rank_share_of_rn",
        "opened_at_rank_max_3",
    )
    .orderBy(F.col("opened_at_seq").asc_nulls_last(), F.col("rn_change").desc(), "id")
    .limit(10)
)
