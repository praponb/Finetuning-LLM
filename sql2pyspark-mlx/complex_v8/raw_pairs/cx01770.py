from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_channel = Window.partitionBy("channel")
w_channel_by_requester_id_movi = Window.partitionBy("channel").orderBy("requester_id", "id").rowsBetween(-3, Window.currentRow)
w_channel_by_closed_at_prior_3 = Window.partitionBy("channel").orderBy(F.col("closed_at").desc(), "id").rowsBetween(-3, -1)
w_channel_by_requester_id = Window.partitionBy("channel").orderBy("requester_id", "id")
w_agent_id_by_requester_id_run = Window.partitionBy("agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_full = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tickets
    .filter((F.col("csat") > 2) & (F.to_date(F.col("opened_at")) < F.lit("2025-03-02").cast("date")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        F.when(F.col("channel").isin("email", "web"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.min("requester_id").over(w_agent_id).alias("agent_id_min_requester_id"),
        F.max("requester_id").over(w_agent_id).alias("agent_id_max_requester_id"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.sum("requester_id").over(w_channel_by_requester_id_movi).alias("requester_id_sum_4"),
        F.round(F.avg("requester_id").over(w_channel_by_closed_at_prior_3), 2).alias("requester_id_avg_3_prior"),
        F.lag("requester_id", 2, 0).over(w_channel_by_requester_id).alias("prev2_requester_id"),
        F.sum("requester_id").over(w_agent_id_by_requester_id_run).alias("running_requester_id"),
        F.last("closed_at").over(w_all_by_closed_at_full).alias("last_closed_at"),
    )
)

enriched = (
    with_windows
    .filter(F.col("running_requester_id") <= 163)
    .select(
        "*",
        (F.col("requester_id_avg_3_prior") - F.col("requester_id_sum_4")).alias("requester_id_avg_3_prior_minus_requester_id_sum_"),
        F.year(F.col("last_closed_at")).alias("last_closed_at_year"),
    )
)

result = (
    enriched
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "channel_group",
        "rows_in_channel",
        "requester_id_sum_4",
        "requester_id_avg_3_prior",
        "last_closed_at",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
        F.lower(F.regexp_replace(F.trim(F.col("channel_group")), " ", "_")).alias("channel_group_slug"),
    )
    .orderBy("channel_group_slug")
)
