from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_closed_at_ties = Window.partitionBy("channel").orderBy("closed_at")
w_channel = Window.partitionBy("channel")
w_channel_len_by_channel_len_r = Window.partitionBy("channel_len").orderBy("channel_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_len_by_channel_len = Window.partitionBy("channel_len").orderBy("channel_len", "id")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_all_by_requester_id_running = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_by_closed_at_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel_len",
        F.round(F.cume_dist().over(w_channel_by_closed_at_ties), 3).alias("closed_at_cume_dist"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel), F.lit(0)), 4).alias("requester_id_share_of_channel"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.sum("requester_id").over(w_channel_len_by_channel_len_r).alias("running_requester_id"),
        (F.col("channel_len") - F.lag("channel_len").over(w_channel_len_by_channel_len)).alias("channel_len_change"),
        F.ntile(4).over(w_all_by_requester_id).alias("requester_id_quartile"),
        F.sum("requester_id").over(w_all_by_requester_id_running).alias("running_requester_id_2"),
        F.round(F.col("channel_len") / F.nullif(F.sum("channel_len").over(w_channel), F.lit(0)), 4).alias("channel_len_share_of_channel"),
        F.ntile(4).over(w_channel_len_by_channel_len).alias("channel_len_quartile"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_channel), 2).alias("requester_id_vs_channel_avg"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.first("channel_len", ignorenulls=True).over(w_channel_by_closed_at_running).alias("first_known_channel_len"),
    )
)

result = (
    metrics
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel_len",
        "closed_at_cume_dist",
        "requester_id_share_of_channel",
        "rows_in_channel",
        "running_requester_id",
        "channel_len_change",
        "requester_id_quartile",
        "channel_len_quartile",
        "requester_id_vs_channel_avg",
        "distinct_agent_id_in_channel",
        "first_known_channel_len",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "channel_len_quartile")
)
