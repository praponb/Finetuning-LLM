from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_tier = Window.partitionBy("csat_tier").orderBy("opened_at", "id")
w_csat_tier_running = Window.partitionBy("csat_tier").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
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
        F.when(F.col("csat") >= 4, "HIGH").when(F.col("csat") >= 3, "MEDIUM").otherwise("LOW").alias("csat_tier"),
    )
)

with_prev = filtered.withColumn("prev_opened_at", F.lag("opened_at").over(w_csat_tier))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_csat_tier_running))

result = (
    sessionized
    .filter(F.col("channel") == "email")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat_tier",
        "prev_opened_at",
        "new_session_flag",
        "burst_id",
        F.lower(F.regexp_replace(F.trim(F.col("csat_tier")), " ", "_")).alias("csat_tier_slug"),
    )
    .orderBy("priority", F.col("closed_at").asc_nulls_last())
)
