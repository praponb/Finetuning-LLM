from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_year_by_opened_at = Window.partitionBy(F.year("opened_at")).orderBy("opened_at")
w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_channel_by_opened_a = Window.partitionBy("priority", "channel").orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_opened_at_year_by_opened_at_2 = Window.partitionBy(F.year("opened_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_requester_id_run = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_requester_id_tie = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last())
w_channel_priority = Window.partitionBy("channel", "priority")
w_agent_id_by_opened_at_prior = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-6, -1)
w_agent_id_by_opened_at_runnin = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_requester_id_ful = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_year = Window.partitionBy(F.year("opened_at"))
w_priority_channel_by_opened_a_2 = Window.partitionBy("priority", "channel").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority = Window.partitionBy("priority")
w_first_known_csat_csat_by_req = Window.partitionBy("first_known_csat", "csat").orderBy(F.col("requester_id_rank").desc())
w_channel = Window.partitionBy("channel")
w_all_by_first_known_requester = Window.orderBy("first_known_requester_id")
w_all_by_max_requester_id_so_f = Window.orderBy(F.col("max_requester_id_so_far").desc(), "id")

base = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-02-04 23:00:00").cast("timestamp")) & (F.col("priority") == "P1"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.col("opened_at") + F.expr("INTERVAL 7 DAYS")).alias("opened_at_plus_7d"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.round(F.cume_dist().over(w_opened_at_year_by_opened_at), 3).alias("opened_at_cume_dist"),
        F.last("csat", ignorenulls=True).over(w_agent_id_csat_by_requester_i).alias("last_known_csat"),
        F.round((F.unix_timestamp("opened_at_plus_7d") - F.unix_timestamp(F.lag("opened_at_plus_7d").over(w_priority_channel_by_opened_a))) / 60, 1).alias("minutes_since_prev_opened_at_plus_7d"),
        F.first("requester_id", ignorenulls=True).over(w_opened_at_year_by_opened_at_2).alias("first_known_requester_id"),
        F.last("opened_at_plus_7d", ignorenulls=True).over(w_agent_id_by_requester_id_run).alias("last_known_opened_at_plus_7d"),
        F.rank().over(w_agent_id_by_requester_id_tie).alias("requester_id_rank"),
        F.round(F.avg("requester_id").over(w_channel_priority), 2).alias("channel_priority_avg_requester_id"),
        F.round(F.avg("csat").over(w_agent_id_by_opened_at_prior), 2).alias("csat_avg_6_prior"),
        F.first("csat", ignorenulls=True).over(w_agent_id_by_opened_at_runnin).alias("first_known_csat"),
        F.nth_value("opened_at_plus_7d", 2).over(w_agent_id_by_requester_id_ful).alias("second_opened_at_plus_7d"),
        F.round(F.avg("csat").over(w_opened_at_year), 2).alias("opened_at_year_avg_csat"),
        F.max("requester_id").over(w_priority_channel_by_opened_a_2).alias("max_requester_id_so_far"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.max("first_known_csat").over(w_priority).alias("priority_max_first_known_csat"),
        F.round(F.col("opened_at_cume_dist") / F.nullif(F.sum("opened_at_cume_dist").over(w_priority), F.lit(0)), 4).alias("opened_at_cume_dist_share_of_priority"),
        F.round(F.cume_dist().over(w_first_known_csat_csat_by_req), 3).alias("requester_id_rank_cume_dist"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.round(F.cume_dist().over(w_all_by_first_known_requester), 3).alias("first_known_requester_id_cume_dist"),
        (F.col("first_known_requester_id") - F.lag("first_known_requester_id").over(w_all_by_max_requester_id_so_f)).alias("first_known_requester_id_change"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "opened_at_plus_7d",
        "opened_at_cume_dist",
        "first_known_requester_id",
        "channel_priority_avg_requester_id",
        "first_known_csat",
        "opened_at_year_avg_csat",
        "priority_max_first_known_csat",
        "opened_at_cume_dist_share_of_priority",
        "requester_id_rank_cume_dist",
        "rows_in_channel",
        "first_known_requester_id_change",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
    .orderBy("opened_at_cume_dist_share_of_priority", "id")
    .limit(20)
)
