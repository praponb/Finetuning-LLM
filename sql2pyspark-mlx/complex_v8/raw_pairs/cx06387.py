from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_dayofweek_agent_id = Window.partitionBy("opened_at_dayofweek", "agent_id")
w_all_by_opened_at_moving_2 = Window.orderBy("opened_at", "id").rowsBetween(-2, Window.currentRow)
w_channel_opened_at_dayofweek = Window.partitionBy("channel", "opened_at_dayofweek").orderBy("csat", "id").rowsBetween(-3, -1)
w_channel_opened_at_dayofweek_2 = Window.partitionBy("channel", "opened_at_dayofweek").orderBy("csat", "id")
w_opened_at_dayofweek = Window.partitionBy("opened_at_dayofweek")
w_channel_opened_at_dayofweek_3 = Window.partitionBy("channel", "opened_at_dayofweek").orderBy("csat")
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_csat_agent_id_by_greatest_cs = Window.partitionBy("csat", "agent_id").orderBy(F.col("greatest_csat_requester_id").desc(), "id")
w_csat_agent_id_by_greatest_cs_2 = Window.partitionBy("csat", "agent_id").orderBy(F.col("greatest_csat_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_to_date_by_greates = Window.partitionBy(F.to_date("opened_at")).orderBy("greatest_csat_requester_id_quintile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_greatest_csa = Window.partitionBy(F.date_trunc("month", "opened_at"), "greatest_csat_requester_id_tercile").orderBy(F.col("prev2_opened_at").asc_nulls_last())
w_last_csat_distinct_channel_i = Window.partitionBy("last_csat", "distinct_channel_in_opened_at_dayofweek_agent_id")
w_greatest_csat_requester_id_q = Window.partitionBy("greatest_csat_requester_id_quintile", F.to_date("opened_at")).orderBy(F.col("csat_percent_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-03-04 11:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.min("csat").over(w_opened_at_dayofweek_agent_id).alias("opened_at_dayofweek_agent_id_min_csat"),
        F.round(F.avg("csat").over(w_all_by_opened_at_moving_2), 2).alias("csat_avg_3"),
        F.round(F.avg("opened_at_dayofweek").over(w_channel_opened_at_dayofweek), 2).alias("opened_at_dayofweek_avg_3_prior"),
        F.size(F.collect_set("channel").over(w_opened_at_dayofweek_agent_id)).alias("distinct_channel_in_opened_at_dayofweek_agent_id"),
        F.round(F.col("greatest_csat_requester_id") / F.nullif(F.sum("greatest_csat_requester_id").over(w_opened_at_dayofweek_agent_id), F.lit(0)), 4).alias("greatest_csat_requester_id_share_of_opened_at_da"),
        (F.col("greatest_csat_requester_id") - F.lag("greatest_csat_requester_id").over(w_channel_opened_at_dayofweek_2)).alias("greatest_csat_requester_id_change"),
        F.round(F.col("opened_at_dayofweek") / F.nullif(F.sum("opened_at_dayofweek").over(w_opened_at_dayofweek), F.lit(0)), 4).alias("opened_at_dayofweek_share_of_opened_at_dayofweek"),
        F.dense_rank().over(w_channel_opened_at_dayofweek_3).alias("csat_dense_rank"),
        F.lag("opened_at", 2).over(w_all_by_opened_at).alias("prev2_opened_at"),
        F.ntile(5).over(w_csat_agent_id_by_greatest_cs).alias("greatest_csat_requester_id_quintile"),
        F.ntile(3).over(w_all_by_opened_at).alias("opened_at_tercile"),
        F.last("csat").over(w_csat_agent_id_by_greatest_cs_2).alias("last_csat"),
        F.ntile(3).over(w_csat_agent_id_by_greatest_cs).alias("greatest_csat_requester_id_tercile"),
        F.round(F.percent_rank().over(w_channel_opened_at_dayofweek_2), 3).alias("csat_percent_rank"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("csat_dense_rank") == 1)
    .drop("csat_dense_rank")
)

metrics_2 = (
    top_per_group
    .select(
        "id",
        "agent_id",
        "channel",
        "tags",
        "opened_at_dayofweek_agent_id_min_csat",
        "csat_avg_3",
        "distinct_channel_in_opened_at_dayofweek_agent_id",
        "greatest_csat_requester_id_share_of_opened_at_da",
        "opened_at_dayofweek_share_of_opened_at_dayofweek",
        "opened_at_tercile",
        "last_csat",
        F.first("last_csat").over(w_opened_at_to_date_by_greates).alias("first_last_csat"),
        F.rank().over(w_opened_at_month_greatest_csa).alias("prev2_opened_at_rank"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_last_csat_distinct_channel_i), F.lit(0)), 4).alias("csat_share_of_last_csat_distinct_channel_in_open"),
        F.last("prev2_opened_at", ignorenulls=True).over(w_greatest_csat_requester_id_q).alias("last_known_prev2_opened_at"),
        F.last("csat_avg_3").over(w_opened_at_to_date_by_greates).alias("last_csat_avg_3"),
        F.first("opened_at_dayofweek").over(w_opened_at_to_date_by_greates).alias("first_opened_at_dayofweek"),
        F.round(F.col("greatest_csat_requester_id") - F.avg("greatest_csat_requester_id").over(w_last_csat_distinct_channel_i), 2).alias("greatest_csat_requester_id_vs_last_csat_distinct"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "channel",
        "tags",
        "opened_at_dayofweek_agent_id_min_csat",
        "greatest_csat_requester_id_share_of_opened_at_da",
        "opened_at_dayofweek_share_of_opened_at_dayofweek",
        "opened_at_tercile",
        "last_csat",
        "first_last_csat",
        "prev2_opened_at_rank",
        "first_opened_at_dayofweek",
        "greatest_csat_requester_id_vs_last_csat_distinct",
    )
    .orderBy(F.col("first_last_csat").asc(), "id")
    .limit(5)
)
