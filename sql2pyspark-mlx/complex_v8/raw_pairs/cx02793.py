from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("agent_id", "channel").orderBy(F.col("new_session_flag").desc(), F.col("id").desc())
w_agent_id_2 = Window.partitionBy("agent_id")
w_csat_prev_opened_at_month_by = Window.partitionBy("csat", F.date_trunc("month", "prev_opened_at")).orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no_channel_by_prev_o = Window.partitionBy("session_no", "channel").orderBy(F.col("prev_opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_by_requester_id = Window.partitionBy("channel").orderBy(F.col("requester_id").desc(), "id")

scoped_tickets = (
    df_tickets
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

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_agent_id_running))

unique_rows = (
    sessionized
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    unique_rows
    .select(
        "*",
        F.size(F.collect_set("priority").over(w_agent_id_2)).alias("distinct_priority_in_agent_id"),
        F.first("closed_at", ignorenulls=True).over(w_csat_prev_opened_at_month_by).alias("first_known_closed_at"),
        F.max("session_no").over(w_session_no_channel_by_prev_o).alias("max_session_no_so_far"),
        F.max("new_session_flag").over(w_agent_id_2).alias("agent_id_max_new_session_flag"),
        F.dense_rank().over(w_channel_by_requester_id).alias("requester_id_dense_rank"),
        F.count(F.lit(1)).over(w_agent_id_2).alias("rows_in_agent_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        "prev_opened_at",
        "new_session_flag",
        "first_known_closed_at",
        "max_session_no_so_far",
        "agent_id_max_new_session_flag",
        "requester_id_dense_rank",
        F.concat_ws(" / ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id_dense_rank").cast("string")).alias("priority_ref"),
    )
    .orderBy(F.col("agent_id").desc(), "id")
    .limit(20)
)
