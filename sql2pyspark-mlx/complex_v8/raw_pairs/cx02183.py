from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
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
        F.abs(F.col("requester_id") - 13).alias("requester_id_distance"),
    )
)

with_prev = base_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_priority_running))

result = (
    sessionized
    .filter(F.col("requester_id") <= 19)
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "csat",
        "prev_opened_at",
        "is_new_session",
        "session_id",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
    .orderBy(F.col("session_id").desc_nulls_first())
)
