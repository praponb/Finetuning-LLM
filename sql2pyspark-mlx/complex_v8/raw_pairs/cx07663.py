from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_moving_2 = Window.orderBy("csat", "id").rowsBetween(-2, Window.currentRow)
w_all_by_csat = Window.orderBy("csat", "id")
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_all_by_csat_rank_running = Window.orderBy(F.col("csat_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat_rank = Window.orderBy(F.col("csat_rank").desc(), "id")

scoped = (
    df_tickets
    .filter(F.col("channel") == "phone")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.round(F.avg("requester_id").over(w_all_by_csat_moving_2), 2).alias("requester_id_avg_3"),
        F.rank().over(w_all_by_csat).alias("csat_rank"),
        F.size(F.collect_set("priority").over(w_agent_id_channel)).alias("distinct_priority_in_agent_id_channel"),
    )
)

filtered = (
    windowed
    .crossJoin(windowed.agg(F.max("requester_id").alias("overall_max_requester_id")))
    .filter(F.col("requester_id") < F.col("overall_max_requester_id"))
    .drop("overall_max_requester_id")
)

ranked = (
    filtered
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "distinct_priority_in_agent_id_channel",
        F.sum("requester_id_avg_3").over(w_all_by_csat_rank_running).alias("running_requester_id_avg_3"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_all_by_csat_rank))) / 60, 1).alias("minutes_since_prev_opened_at"),
    )
)

result = (
    ranked
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "distinct_priority_in_agent_id_channel",
    )
    .orderBy(F.col("priority").desc(), F.col("channel").asc())
)
