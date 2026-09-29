from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.month(F.col("opened_at")).isin(1, 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

with_prev = filtered.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

annotated = (
    sessionized
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "days_since_opened_at",
        "prev_opened_at",
        "new_session_flag",
        "burst_id",
        F.coalesce(F.col("burst_id"), F.lit(0)).alias("burst_id_filled"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

result = (
    annotated
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "days_since_opened_at",
        "prev_opened_at",
        "new_session_flag",
        "burst_id",
        "burst_id_filled",
        "requester_id_bucket",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("prev_opened_at"))) / 60, 1).alias("minutes_prev_opened_at_to_closed_at"),
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id_bucket").cast("string")).alias("priority_ref"),
    )
    .orderBy(F.col("closed_at").desc(), "requester_id_bucket")
)
