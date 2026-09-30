from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_requester_id_tier_b = Window.partitionBy("agent_id", "requester_id_tier").orderBy("opened_at", "id")
w_closed_at_month_agent_id = Window.partitionBy(F.month("closed_at"), "agent_id")
w_agent_id_requester_id_tier_b_2 = Window.partitionBy("agent_id", "requester_id_tier").orderBy("opened_at")
w_opened_at_rank = Window.partitionBy("opened_at_rank")
w_opened_at_rank_channel_or_un = Window.partitionBy("opened_at_rank", "channel_or_unknown").orderBy(F.col("seq_no").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter((F.col("channel") == "web") & (F.hour(F.col("opened_at")).between(8, 13)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        F.when(F.col("requester_id") >= 22, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.row_number().over(w_agent_id_requester_id_tier_b).alias("seq_no"),
        F.round(F.col("requester_id_bucket") - F.avg("requester_id_bucket").over(w_closed_at_month_agent_id), 2).alias("requester_id_bucket_vs_closed_at_month_agent_id_"),
        F.rank().over(w_agent_id_requester_id_tier_b).alias("opened_at_rank"),
        F.min("requester_id_bucket").over(w_closed_at_month_agent_id).alias("closed_at_month_agent_id_min_requester_id_bucket"),
        F.round(F.cume_dist().over(w_agent_id_requester_id_tier_b_2), 3).alias("opened_at_cume_dist"),
        F.round(F.avg("requester_id_bucket").over(w_closed_at_month_agent_id), 2).alias("closed_at_month_agent_id_avg_requester_id_bucket"),
    )
)

first_rows = windowed.filter(F.col("opened_at_rank") == 1)

with_trends = (
    first_rows
    .select(
        "id",
        "opened_at",
        "closed_at",
        "tags",
        "channel_or_unknown",
        "seq_no",
        "requester_id_bucket_vs_closed_at_month_agent_id_",
        "opened_at_rank",
        "closed_at_month_agent_id_min_requester_id_bucket",
        "opened_at_cume_dist",
        "closed_at_month_agent_id_avg_requester_id_bucket",
        F.size(F.collect_set("requester_id_tier").over(w_opened_at_rank)).alias("distinct_requester_id_tier_in_opened_at_rank"),
        F.sum("opened_at_cume_dist").over(w_opened_at_rank_channel_or_un).alias("running_opened_at_cume_dist"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["opened_at_rank", "channel_or_unknown"], ["opened_at_rank"], ["channel_or_unknown"]],
        "opened_at_rank", "channel_or_unknown",
    )
    .agg(F.max("closed_at").alias("last_closed_at"))
    .orderBy(
        F.col("opened_at_rank").asc_nulls_last(),
        F.col("channel_or_unknown").asc_nulls_last(),
    )
)
