from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_ties = Window.orderBy(F.col("requester_id").desc())
w_all_by_requester_id_running = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_channel = Window.partitionBy("priority", "channel")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_latest = Window.partitionBy("channel").orderBy(F.col("running_count").desc(), F.col("id").desc())

base = (
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
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_priority_running))

ranked = (
    sessionized
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_requester_id_ties), 3).alias("requester_id_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_requester_id_running).alias("running_count"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_priority_channel), F.lit(0)), 4).alias("requester_id_share_of_priority_channel"),
        F.max("session_id").over(w_priority_channel).alias("priority_channel_max_session_id"),
        F.row_number().over(w_all_by_requester_id).alias("seq_no"),
    )
)

deduped = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupingSets(
        [["priority", "is_new_session"], ["priority"], ["is_new_session"], []],
        "priority", "is_new_session",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("is_new_session").asc_nulls_last(),
    )
)
