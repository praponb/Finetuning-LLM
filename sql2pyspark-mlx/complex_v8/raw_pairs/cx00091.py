from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat = Window.orderBy("csat", "id")
w_opened_at_month_by_requester = Window.partitionBy(F.month("opened_at")).orderBy("requester_id_bucket", "id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")

valid_tickets = (
    df_tickets
    .filter((F.col("closed_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")) | (F.size(F.col("tags")) >= 2))
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
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

with_trends = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "requester_id_bucket",
        F.lag("requester_id_bucket", 2).over(w_all_by_csat).alias("prev2_requester_id_bucket"),
        F.lag("closed_at", 3).over(w_opened_at_month_by_requester).alias("prev3_closed_at"),
        F.lead("opened_at").over(w_all_by_csat).alias("next_opened_at"),
        F.round(F.col("requester_id_bucket") / F.nullif(F.sum("requester_id_bucket").over(w_agent_id_csat), F.lit(0)), 4).alias("requester_id_bucket_share_of_agent_id_csat"),
    )
)

result = (
    with_trends
    .rollup("csat", "channel")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("prev2_requester_id_bucket") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("prev2_requester_id_bucket_to_requester_id_ratio"),
        F.sum("requester_id").alias("total_requester_id"),
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("channel").asc_nulls_last())
)
