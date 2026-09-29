from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_csat_by_opened_at = Window.partitionBy("channel", "csat").orderBy("opened_at", "id")
w_agent_id_channel_by_opened_a = Window.partitionBy("agent_id", "channel").orderBy(F.col("opened_at_month").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_channel_csat_by_opened_at_fu = Window.partitionBy("channel", "csat").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_year = Window.partitionBy(F.year("opened_at_month"))
w_channel_csat_by_opened_at_ti = Window.partitionBy("channel", "csat").orderBy("opened_at")
w_agent_id_channel_by_opened_a_2 = Window.partitionBy("agent_id", "channel").orderBy(F.col("opened_at_month").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month_second_csat = Window.partitionBy(F.month("opened_at"), "second_csat")
w_csat_second_csat_by_requeste = Window.partitionBy("csat", "second_csat").orderBy(F.col("requester_id").desc())
w_csat_first_opened_at_month_m = Window.partitionBy("csat", F.date_trunc("month", "first_opened_at_month")).orderBy(F.col("requester_id_vs_opened_at_month_year_avg").asc_nulls_last(), "id")
w_csat_second_csat_by_requeste_2 = Window.partitionBy("csat", "second_csat").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_opened_at_ties = Window.orderBy("last_opened_at")
w_distinct_channel_in_opened_a = Window.partitionBy("distinct_channel_in_opened_at_month_year")

filtered_tickets = (
    df_tickets
    .filter((F.col("requester_id") < 21) & (F.col("priority").isin("P2", "P4", "P1")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

metrics = (
    filtered_tickets
    .select(
        "id",
        "requester_id",
        "opened_at",
        "channel",
        "csat",
        "requester_id_filled",
        F.lag("csat", 1, 0).over(w_channel_csat_by_opened_at).alias("prev_csat"),
        F.sum("requester_id").over(w_agent_id_channel_by_opened_a).alias("requester_id_sum_3"),
        F.first("opened_at_month").over(w_channel_csat_by_opened_at_fu).alias("first_opened_at_month"),
        F.nth_value("csat", 2).over(w_channel_csat_by_opened_at_fu).alias("second_csat"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_opened_at_month_year), 2).alias("requester_id_vs_opened_at_month_year_avg"),
        F.size(F.collect_set("channel").over(w_opened_at_month_year)).alias("distinct_channel_in_opened_at_month_year"),
        F.round(F.cume_dist().over(w_channel_csat_by_opened_at_ti), 3).alias("opened_at_cume_dist"),
        F.first("requester_id", ignorenulls=True).over(w_agent_id_channel_by_opened_a_2).alias("first_known_requester_id"),
        F.lag("requester_id_filled", 2).over(w_channel_csat_by_opened_at).alias("prev2_requester_id_filled"),
        F.round(F.col("requester_id_filled") - F.avg("requester_id_filled").over(w_opened_at_month_year), 2).alias("requester_id_filled_vs_opened_at_month_year_avg"),
    )
)

enriched = (
    metrics
    .filter(F.col("channel") == "chat")
    .select(
        "*",
        F.greatest(F.col("opened_at_cume_dist"), F.col("requester_id_filled")).alias("greatest_opened_at_cume_dist_requester_id_filled"),
        F.round(F.col("opened_at_cume_dist") * 1.1, 2).alias("opened_at_cume_dist_adj"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.min("csat").over(w_opened_at_month_second_csat).alias("opened_at_month_second_csat_min_csat"),
        F.sum("opened_at_cume_dist_adj").over(w_opened_at_month_second_csat).alias("opened_at_month_second_csat_sum_opened_at_cume_d"),
        F.round(F.col("first_known_requester_id") - F.avg("first_known_requester_id").over(w_opened_at_month_second_csat), 2).alias("first_known_requester_id_vs_opened_at_month_seco"),
        F.rank().over(w_csat_second_csat_by_requeste).alias("requester_id_rank"),
        F.rank().over(w_csat_first_opened_at_month_m).alias("requester_id_vs_opened_at_month_year_avg_rank"),
        F.count(F.lit(1)).over(w_opened_at_month_second_csat).alias("rows_in_opened_at_month_second_csat"),
        F.max("csat").over(w_csat_second_csat_by_requeste_2).alias("max_csat_so_far"),
        F.sum("opened_at_cume_dist").over(w_opened_at_month_second_csat).alias("opened_at_month_second_csat_sum_opened_at_cume_d_2"),
    )
)

per_group = (
    sequenced
    .filter(F.col("distinct_channel_in_opened_at_month_year") > 5)
    .groupBy("distinct_channel_in_opened_at_month_year")
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("channel").isin("chat", "web"), F.col("prev2_requester_id_filled"))).alias("prev2_requester_id_filled_channel_in_chat_web"),
        F.round(100 * F.count(F.when(F.col("channel") != "phone", 1)) / F.count("*"), 1).alias("pct_channel_phone"),
        F.max("opened_at").alias("last_opened_at"),
        F.sum(F.when(~F.col("channel").isin("web", "email"), F.col("prev_csat")).otherwise(0)).alias("prev_csat_channel_not_in_web_email"),
        F.avg("requester_id_sum_3").alias("avg_requester_id_sum_3_check"),
    )
    .filter(F.col("avg_requester_id_sum_3_check") > 23)
    .drop("avg_requester_id_sum_3_check")
)

with_windows = (
    per_group
    .select(
        "*",
        F.rank().over(w_all_by_last_opened_at_ties).alias("last_opened_at_rank"),
        F.count(F.lit(1)).over(w_distinct_channel_in_opened_a).alias("rows_in_distinct_channel_in_opened_at_month_year"),
    )
)

result = (
    with_windows
    .select(
        "distinct_channel_in_opened_at_month_year",
        "channel_list",
        "last_opened_at",
        "prev_csat_channel_not_in_web_email",
        "last_opened_at_rank",
        "rows_in_distinct_channel_in_opened_at_month_year",
        (F.floor(F.col("last_opened_at_rank") / 2) * 2).alias("last_opened_at_rank_bucket"),
    )
    .orderBy(
        "distinct_channel_in_opened_at_month_year",
        F.col("last_opened_at_rank_bucket").asc_nulls_last(),
    )
)
