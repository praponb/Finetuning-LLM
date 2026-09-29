from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_closed_at = Window.partitionBy("channel").orderBy(F.col("closed_at").desc(), "id")
w_channel = Window.partitionBy("channel")
w_all_by_priority_len_ties = Window.orderBy("priority_len")

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

windowed = (
    filtered_tickets
    .select(
        "*",
        F.row_number().over(w_channel_by_closed_at).alias("seq_no"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel), F.lit(0)), 4).alias("csat_share_of_channel"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.round(F.cume_dist().over(w_all_by_priority_len_ties), 3).alias("priority_len_cume_dist"),
        F.sum("csat").over(w_channel).alias("channel_sum_csat"),
        F.round(F.col("days_since_opened_at") / F.nullif(F.sum("days_since_opened_at").over(w_channel), F.lit(0)), 4).alias("days_since_opened_at_share_of_channel"),
    )
)

per_label = (
    windowed
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "opened_at_week",
        "priority_len",
        "days_since_opened_at",
        "seq_no",
        "csat_share_of_channel",
        "distinct_agent_id_in_channel",
        "priority_len_cume_dist",
        "channel_sum_csat",
        "days_since_opened_at_share_of_channel",
        F.explode_outer("tags").alias("tag"),
    )
)

derived = (
    per_label
    .filter(F.col("distinct_agent_id_in_channel") > 5)
    .select(
        "*",
        F.date_trunc("day", F.col("closed_at")).alias("closed_at_day"),
        F.quarter(F.col("opened_at_week")).alias("opened_at_week_quarter"),
    )
)

result = (
    derived
    .groupBy("channel")
    .pivot("tag", ["bug", "upgrade", "login"])
    .agg(F.avg("days_since_opened_at"))
)
