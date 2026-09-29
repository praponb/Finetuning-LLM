from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_group_opened_at_to = Window.partitionBy("priority_group", F.to_date("opened_at")).orderBy(F.col("opened_at").desc(), "id")
w_csat_priority_group = Window.partitionBy("csat", "priority_group")
w_priority_group_channel_by_cl = Window.partitionBy("priority_group", "channel").orderBy(F.col("closed_at").asc_nulls_last(), "id")
w_priority_group_channel_by_cl_2 = Window.partitionBy("priority_group", "channel").orderBy(F.col("closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")

cleaned_tickets = (
    df_tickets
    .filter((F.col("channel") == "phone") | (F.col("opened_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")))
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
        F.when(F.col("priority").isin("P3", "P4"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.when(F.col("channel").isin("email", "phone"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
    )
)

with_windows = (
    cleaned_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "priority_group",
        "channel_group",
        F.round(F.percent_rank().over(w_priority_group_opened_at_to), 3).alias("opened_at_percent_rank"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_priority_group), F.lit(0)), 4).alias("csat_share_of_csat_priority_group"),
        F.row_number().over(w_priority_group_opened_at_to).alias("rn"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        "channel_group",
        "opened_at_percent_rank",
        "csat_share_of_csat_priority_group",
        "rn",
        F.ntile(10).over(w_priority_group_channel_by_cl).alias("closed_at_decile"),
        F.min("rn").over(w_priority_group_channel_by_cl_2).alias("min_rn_so_far"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
        F.lag("opened_at_percent_rank", 2).over(w_priority_group_channel_by_cl).alias("prev2_opened_at_percent_rank"),
    )
)

result = (
    with_trends
    .rollup("rn", "closed_at_decile")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("closed_at_decile").alias("max_closed_at_decile"),
        F.count("*").alias("row_count"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
    )
)
