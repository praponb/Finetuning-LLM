from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id_bucket = Window.orderBy("requester_id_bucket", "id")
w_channel_or_unknown_priority = Window.partitionBy("channel_or_unknown", "priority")
w_priority_group_by_closed_at = Window.partitionBy("priority_group").orderBy("closed_at", "id")
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_priority_group_priority = Window.partitionBy("priority_group", "priority")
w_channel_by_prev_closed_at = Window.partitionBy("channel").orderBy(F.col("prev_closed_at").desc(), "id")
w_all_by_prev2_closed_at = Window.orderBy(F.col("prev2_closed_at").desc(), "id")

base = (
    df_tickets
    .filter((F.col("agent_id") <= 3) & (F.hour(F.col("closed_at")).between(6, 9)))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
    )
)

annotated = (
    base
    .filter(F.col("requester_id_bucket").between(3, 20))
    .select(
        "*",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.when(F.col("priority").isin("P2", "P4"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix_2"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_requester_id_bucket), 3).alias("requester_id_bucket_cume_dist"),
        F.round(F.col("requester_id_bucket") - F.avg("requester_id_bucket").over(w_channel_or_unknown_priority), 2).alias("requester_id_bucket_vs_channel_or_unknown_priori"),
        F.lag("closed_at", 2).over(w_priority_group_by_closed_at).alias("prev2_closed_at"),
        F.lag("closed_at").over(w_all_by_closed_at).alias("prev_closed_at"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "priority",
        "channel",
        "tags",
        "requester_id_bucket",
        "channel_prefix",
        "channel_or_unknown",
        "priority_group",
        "channel_prefix_2",
        "requester_id_bucket_cume_dist",
        F.round(F.avg("requester_id_bucket").over(w_priority_group_priority), 2).alias("priority_group_priority_avg_requester_id_bucket"),
        F.ntile(5).over(w_channel_by_prev_closed_at).alias("prev_closed_at_quintile"),
        F.size(F.collect_set("channel").over(w_priority_group_priority)).alias("distinct_channel_in_priority_group_priority"),
        F.ntile(10).over(w_all_by_prev2_closed_at).alias("prev2_closed_at_decile"),
        F.lead("closed_at", 2).over(w_all_by_prev2_closed_at).alias("next2_closed_at"),
    )
)

result = (
    ranked_2
    .rollup("prev2_closed_at_decile", "priority")
    .agg(
        F.sum("prev_closed_at_quintile").alias("total_prev_closed_at_quintile"),
    )
    .orderBy(
        F.col("prev2_closed_at_decile").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
