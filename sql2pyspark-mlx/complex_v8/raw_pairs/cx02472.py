from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_channel_group_by_opened = Window.partitionBy("csat", "channel_group").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_agent_id = Window.partitionBy(F.month("opened_at"), "agent_id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_channel_group_by_csat = Window.partitionBy("channel_group").orderBy("csat", "id")
w_channel_group_by_csat_full = Window.partitionBy("channel_group").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_channel_group_by_opened_2 = Window.partitionBy("csat", "channel_group").orderBy(F.col("opened_at").asc_nulls_last())

filtered = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(10, 13)) & (F.col("priority") == "P4"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.when(F.col("channel").isin("web", "email"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        F.nth_value("opened_at", 2).over(w_csat_channel_group_by_opened).alias("second_opened_at"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_opened_at_month_agent_id), F.lit(0)), 4).alias("csat_share_of_opened_at_month_agent_id"),
        F.min("csat").over(w_agent_id_csat).alias("agent_id_csat_min_csat"),
        F.round(F.avg("requester_id").over(w_agent_id_csat), 2).alias("agent_id_csat_avg_requester_id"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_opened_at_month_agent_id), F.lit(0)), 4).alias("requester_id_share_of_opened_at_month_agent_id"),
        F.count(F.lit(1)).over(w_opened_at_month_agent_id).alias("rows_in_opened_at_month_agent_id"),
        F.ntile(4).over(w_channel_group_by_csat).alias("csat_quartile"),
        F.round(F.col("csat") - F.avg("csat").over(w_opened_at_month_agent_id), 2).alias("csat_vs_opened_at_month_agent_id_avg"),
        F.first("requester_id").over(w_channel_group_by_csat_full).alias("first_requester_id"),
        F.round(F.percent_rank().over(w_csat_channel_group_by_opened_2), 3).alias("opened_at_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        "second_opened_at",
        "csat_share_of_opened_at_month_agent_id",
        "agent_id_csat_min_csat",
        "agent_id_csat_avg_requester_id",
        "rows_in_agent_id_csat",
        "csat_vs_opened_at_month_agent_id_avg",
        "first_requester_id",
        "opened_at_percent_rank",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.when(F.col("opened_at_percent_rank") >= 0.8, "large").when(F.col("opened_at_percent_rank") >= 0.5, "medium").otherwise("small").alias("opened_at_percent_rank_tier"),
    )
    .orderBy(F.col("opened_at").desc(), F.col("priority").desc_nulls_first())
)
