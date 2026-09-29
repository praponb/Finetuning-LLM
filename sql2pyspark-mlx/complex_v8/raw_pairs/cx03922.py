from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_agent_id_closed_at_month = Window.partitionBy("agent_id", F.date_trunc("month", "closed_at"))
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_requester_id_mov = Window.partitionBy("agent_id").orderBy("requester_id", "id").rowsBetween(-6, Window.currentRow)
w_all_by_closed_at_running = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy("requester_id", "id")
w_agent_id_by_requester_id_ful = Window.partitionBy("agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tickets
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-03-04").cast("date"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        F.concat_ws("|", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel_priority_label",
        F.nth_value("closed_at", 2).over(w_all_by_opened_at_full).alias("second_closed_at"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_id"),
        F.ntile(5).over(w_all_by_opened_at).alias("opened_at_quintile"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id_closed_at_month), 2).alias("requester_id_vs_agent_id_closed_at_month_avg"),
        F.last("closed_at").over(w_all_by_closed_at_full).alias("last_closed_at"),
        F.first("closed_at").over(w_all_by_opened_at_full).alias("first_closed_at"),
        F.last("closed_at").over(w_all_by_opened_at_full).alias("last_closed_at_3"),
        F.nth_value("requester_id", 2).over(w_all_by_closed_at_full).alias("second_requester_id"),
        F.round(F.avg("requester_id").over(w_agent_id_by_requester_id_mov), 2).alias("requester_id_avg_7"),
        F.last("closed_at", ignorenulls=True).over(w_all_by_closed_at_running).alias("last_known_closed_at"),
        (F.col("requester_id") - F.lag("requester_id").over(w_agent_id_by_requester_id)).alias("requester_id_change"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
        F.nth_value("requester_id", 2).over(w_agent_id_by_requester_id_ful).alias("second_requester_id_2"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_closed_at_month), F.lit(0)), 4).alias("requester_id_share_of_agent_id_closed_at_month"),
    )
)

stats = (
    windowed
    .groupBy(
        F.date_trunc("month", "last_known_closed_at").alias("last_known_closed_at_month"),
        "agent_id",
        F.date_trunc("month", "first_closed_at").alias("first_closed_at_month"),
    )
    .agg(
        F.min("last_closed_at_3").alias("first_last_closed_at_3"),
        F.max("second_closed_at").alias("last_second_closed_at"),
        F.round(F.sum("requester_id_share_of_agent_id"), 2).alias("total_requester_id_share_of_agent_id"),
        F.min("second_closed_at").alias("first_second_closed_at"),
    )
)

result = (
    stats
    .select(
        "last_known_closed_at_month",
        "first_closed_at_month",
        "first_last_closed_at_3",
        "last_second_closed_at",
        "first_second_closed_at",
    )
    .orderBy("last_second_closed_at")
)
