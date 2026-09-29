from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_csat_opened_at_month_month_b = Window.partitionBy("csat", F.month("opened_at_month")).orderBy(F.col("total_requester_id").desc_nulls_first(), "channel", "opened_at_month", "csat")
w_channel_by_total_requester_i = Window.partitionBy("channel").orderBy("total_requester_id_vs_channel_avg")
w_distinct_ids_csat_by_total_r = Window.partitionBy("distinct_ids", "csat").orderBy(F.col("total_requester_id_percent_rank").desc(), "channel", "opened_at_month", "csat").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tickets = (
    df_tickets
    .filter(F.col("requester_id") <= 20)
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
    )
)

grouped = (
    prepared_tickets
    .filter(F.col("requester_id") > 5)
    .groupBy(
        "channel",
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
        "csat",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "email", F.col("csat")).otherwise(0)).alias("csat_channel_email"),
        F.min("requester_id").alias("min_requester_id"),
        F.max("opened_at").alias("last_opened_at"),
        F.sum("requester_id").alias("total_requester_id"),
        F.round(F.sum("csat") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("csat_to_requester_id_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("requester_id").alias("avg_requester_id_check"),
    )
    .filter(F.col("avg_requester_id_check") > 7)
    .drop("avg_requester_id_check")
)

windowed = (
    grouped
    .select(
        "channel",
        "opened_at_month",
        "csat",
        "csat_channel_email",
        "last_opened_at",
        "total_requester_id",
        "csat_to_requester_id_ratio",
        "distinct_ids",
        F.min("min_requester_id").over(w_channel).alias("channel_min_min_requester_id"),
        F.round(F.col("total_requester_id") - F.avg("total_requester_id").over(w_channel), 2).alias("total_requester_id_vs_channel_avg"),
        F.round(F.percent_rank().over(w_csat_opened_at_month_month_b), 3).alias("total_requester_id_percent_rank"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.dense_rank().over(w_channel_by_total_requester_i).alias("total_requester_id_vs_channel_avg_dense_rank"),
        F.nth_value("csat", 2).over(w_distinct_ids_csat_by_total_r).alias("second_csat"),
    )
)

derived = (
    sequenced
    .filter(F.col("second_csat") > 2)
    .select(
        "*",
        F.coalesce(F.col("second_csat"), F.lit(0)).alias("second_csat_filled"),
        F.round(F.col("csat_channel_email") / F.nullif(F.col("total_requester_id"), F.lit(0)), 3).alias("csat_channel_email_per_total_requester_id"),
    )
)

tagged = (
    derived
    .filter(F.col("csat") >= 3)
    .select(
        "channel",
        "opened_at_month",
        "csat",
        "csat_channel_email",
        "last_opened_at",
        "total_requester_id",
        "csat_to_requester_id_ratio",
        "distinct_ids",
        "total_requester_id_vs_channel_avg",
        "second_csat",
        "second_csat_filled",
        "csat_channel_email_per_total_requester_id",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

result = (
    tagged
    .groupingSets(
        [["second_csat", "distinct_ids"], ["second_csat"], ["distinct_ids"], []],
        "second_csat", "distinct_ids",
    )
    .agg(F.sum("second_csat").alias("total_second_csat"))
    .orderBy(
        F.col("second_csat").asc_nulls_last(),
        F.col("distinct_ids").asc_nulls_last(),
    )
)
