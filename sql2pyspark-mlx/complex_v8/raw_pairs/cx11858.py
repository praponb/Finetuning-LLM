from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_channel_by_csat_ties = Window.partitionBy("channel").orderBy(F.col("csat").desc())
w_channel_by_csat_running = Window.partitionBy("channel").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_closed_at = Window.partitionBy("priority").orderBy("closed_at", "id")
w_opened_at_year_by_requester = Window.partitionBy(F.year("opened_at")).orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_year_distinct_chan = Window.partitionBy(F.year("opened_at"), "distinct_channel_in_agent_id_csat")
w_closed_at_year_first_known_c = Window.partitionBy(F.year("closed_at"), F.date_trunc("month", "first_known_closed_at")).orderBy(F.col("csat_share_of_agent_id_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_channel_in_agent_id = Window.partitionBy("distinct_channel_in_agent_id_csat", F.to_date("first_known_closed_at")).orderBy("csat", "id").rowsBetween(-2, Window.currentRow)
w_closed_at_year_first_known_c_2 = Window.partitionBy(F.year("closed_at"), F.date_trunc("month", "first_known_closed_at")).orderBy(F.col("csat_share_of_agent_id_csat").desc())
w_csat_distinct_channel_in_age = Window.partitionBy("csat", "distinct_channel_in_agent_id_csat").orderBy("closed_at", "id")
w_csat_distinct_channel_in_age_2 = Window.partitionBy("csat", "distinct_channel_in_agent_id_csat").orderBy("closed_at")
w_distinct_channel_in_agent_id_2 = Window.partitionBy("distinct_channel_in_agent_id_csat", F.to_date("first_known_closed_at")).orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tickets
    .filter(F.col("requester_id") < 23)
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

windowed = (
    scoped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_csat), 2).alias("csat_vs_agent_id_csat_avg"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_csat), F.lit(0)), 4).alias("csat_share_of_agent_id_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_csat), F.lit(0)), 4).alias("requester_id_share_of_agent_id_csat"),
        F.round(F.cume_dist().over(w_channel_by_csat_ties), 3).alias("csat_cume_dist"),
        F.sum("requester_id").over(w_channel_by_csat_running).alias("running_requester_id"),
        F.lag("requester_id").over(w_priority_by_closed_at).alias("prev_requester_id"),
        F.nth_value("closed_at", 2).over(w_opened_at_year_by_requester).alias("second_closed_at"),
        F.max("csat").over(w_channel_by_csat_running).alias("max_csat_so_far"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.first("closed_at", ignorenulls=True).over(w_channel_by_csat_running).alias("first_known_closed_at"),
        F.size(F.collect_set("channel").over(w_agent_id_csat)).alias("distinct_channel_in_agent_id_csat"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "tags",
        "requester_id_share_of_agent_id_csat",
        "csat_cume_dist",
        "running_requester_id",
        "prev_requester_id",
        "max_csat_so_far",
        "distinct_channel_in_agent_id_csat",
        F.round(F.avg("prev_requester_id").over(w_opened_at_year_distinct_chan), 2).alias("opened_at_year_distinct_channel_in_agent_id_csat"),
        F.nth_value("csat_share_of_agent_id_csat", 2).over(w_closed_at_year_first_known_c).alias("second_csat_share_of_agent_id_csat"),
        F.round(F.avg("max_csat_so_far").over(w_distinct_channel_in_agent_id), 2).alias("max_csat_so_far_avg_3"),
        F.rank().over(w_closed_at_year_first_known_c_2).alias("csat_share_of_agent_id_csat_rank"),
        F.rank().over(w_csat_distinct_channel_in_age).alias("closed_at_rank"),
        F.dense_rank().over(w_csat_distinct_channel_in_age_2).alias("closed_at_dense_rank"),
        F.nth_value("max_csat_so_far", 2).over(w_distinct_channel_in_agent_id_2).alias("second_max_csat_so_far"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "tags",
        "requester_id_share_of_agent_id_csat",
        "csat_cume_dist",
        "running_requester_id",
        "prev_requester_id",
        "max_csat_so_far",
        "distinct_channel_in_agent_id_csat",
        "opened_at_year_distinct_channel_in_agent_id_csat",
        "second_csat_share_of_agent_id_csat",
        "max_csat_so_far_avg_3",
        "csat_share_of_agent_id_csat_rank",
        "closed_at_rank",
        "closed_at_dense_rank",
        "second_max_csat_so_far",
        (F.col("closed_at") + F.expr("INTERVAL 7 DAYS")).alias("closed_at_plus_7d"),
    )
    .orderBy("closed_at", F.col("distinct_channel_in_agent_id_csat").asc(), "id")
    .limit(20)
)
