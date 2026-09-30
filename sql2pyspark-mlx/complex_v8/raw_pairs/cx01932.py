from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_channel_opened_at_month_by_r = Window.partitionBy("channel", F.date_trunc("month", "opened_at")).orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_opened_at_month_by_r_2 = Window.partitionBy("channel", F.date_trunc("month", "opened_at")).orderBy("requester_id", "id")
w_csat_channel_or_unknown_by_o = Window.partitionBy("csat", "channel_or_unknown").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_channel_or_unknown_by_o_2 = Window.partitionBy("csat", "channel_or_unknown").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tickets
    .filter((F.col("priority") != "P1") & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "requester_id",
        "opened_at",
        "channel",
        "tags",
        "channel_or_unknown",
        F.size(F.collect_set("channel_or_unknown").over(w_channel)).alias("distinct_channel_or_unknown_in_channel"),
        F.first("requester_id").over(w_channel_opened_at_month_by_r).alias("first_requester_id"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_channel_opened_at_month_by_r_2))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.sum("csat").over(w_channel).alias("channel_sum_csat"),
        F.first("csat").over(w_channel_opened_at_month_by_r).alias("first_csat"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.sum("requester_id").over(w_channel).alias("channel_sum_requester_id"),
        F.last("requester_id", ignorenulls=True).over(w_csat_channel_or_unknown_by_o).alias("last_known_requester_id"),
        F.lead("requester_id", 1, 0).over(w_channel_opened_at_month_by_r_2).alias("next_requester_id"),
        F.nth_value("csat", 2).over(w_csat_channel_or_unknown_by_o_2).alias("second_csat"),
        F.last("opened_at", ignorenulls=True).over(w_csat_channel_or_unknown_by_o).alias("last_known_opened_at"),
    )
)

summary = (
    metrics
    .filter(F.col("channel_sum_requester_id") > 101)
    .groupBy("channel")
    .agg(
        F.percentile_approx("next_requester_id", 0.5).alias("median_next_requester_id"),
        F.sum("last_known_requester_id").alias("total_last_known_requester_id"),
        F.sum("channel_sum_csat").alias("total_channel_sum_csat"),
    )
)

result = (
    summary
    .filter(F.col("total_channel_sum_csat") >= 121)
    .select(
        "channel",
        "median_next_requester_id",
        "total_last_known_requester_id",
        (F.col("median_next_requester_id") - F.col("total_channel_sum_csat")).alias("median_next_requester_id_minus_total_channel_sum"),
    )
    .orderBy(F.col("median_next_requester_id").desc())
)
