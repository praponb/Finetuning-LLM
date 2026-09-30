from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_closed_at_year_distinct = Window.partitionBy(F.year("last_closed_at"), "distinct_channels")
w_all_by_requester_id_channel = Window.orderBy(F.col("requester_id_channel_phone").desc(), "closed_at_day")
w_distinct_channels = Window.partitionBy("distinct_channels")
w_distinct_channels_by_distinc = Window.partitionBy("distinct_channels").orderBy("distinct_channels", "closed_at_day").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_channels = Window.orderBy(F.col("distinct_channels").desc(), "closed_at_day")

recent = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

grouped = (
    recent
    .groupBy(F.to_date("closed_at").alias("closed_at_day"))
    .agg(
        F.sum(F.when(F.col("channel") == "phone", F.col("requester_id")).otherwise(0)).alias("requester_id_channel_phone"),
        F.max("closed_at").alias("last_closed_at"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)

with_activity = (
    grouped
    .crossJoin(grouped.agg(F.avg("requester_id_channel_phone").alias("overall_avg_requester_id_channel_phone")))
    .filter(F.col("requester_id_channel_phone") > F.col("overall_avg_requester_id_channel_phone"))
    .drop("overall_avg_requester_id_channel_phone")
)

ranked = (
    with_activity
    .select(
        "*",
        F.round(F.avg("requester_id_channel_phone").over(w_last_closed_at_year_distinct), 2).alias("last_closed_at_year_distinct_channels_avg_reques"),
        F.lead("closed_at_day", 2).over(w_all_by_requester_id_channel).alias("next2_closed_at_day"),
    )
)

scored = (
    ranked
    .select(
        "closed_at_day",
        "requester_id_channel_phone",
        "distinct_channels",
        F.date_format(F.col("closed_at_day"), "yyyy-MM-dd").alias("closed_at_day_label"),
    )
)

screened = (
    scored
    .crossJoin(scored.agg(F.avg("requester_id_channel_phone").alias("overall_avg_requester_id_channel_phone")))
    .filter(F.col("requester_id_channel_phone") > F.col("overall_avg_requester_id_channel_phone"))
    .drop("overall_avg_requester_id_channel_phone")
)

with_windows = (
    screened
    .select(
        "*",
        F.min("distinct_channels").over(w_distinct_channels).alias("distinct_channels_min_distinct_channels"),
        F.last("requester_id_channel_phone").over(w_distinct_channels_by_distinc).alias("last_requester_id_channel_phone"),
        F.max("distinct_channels").over(w_distinct_channels).alias("distinct_channels_max_distinct_channels"),
        F.lag("distinct_channels", 2, 0).over(w_all_by_distinct_channels).alias("prev2_distinct_channels"),
        F.count(F.lit(1)).over(w_distinct_channels).alias("rows_in_distinct_channels"),
        F.lag("closed_at_day", 3).over(w_all_by_distinct_channels).alias("prev3_closed_at_day"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["prev2_distinct_channels", "distinct_channels"], ["prev2_distinct_channels"]],
        "prev2_distinct_channels", "distinct_channels",
    )
    .agg(F.max("closed_at_day").alias("last_closed_at_day"))
)
