from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_csat_by_prev_opened = Window.partitionBy("priority", "csat").orderBy("prev_opened_at", "id").rowsBetween(-3, Window.currentRow)
w_csat = Window.partitionBy("csat")

scoped = (
    df_tickets
    .filter(F.col("priority") != "P1")
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
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
    )
)

with_prev = scoped.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_channel_running))

sequenced = (
    sessionized
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "burst_id",
        F.round(F.avg("requester_id").over(w_priority_csat_by_prev_opened), 2).alias("requester_id_avg_4"),
        F.max("burst_id").over(w_csat).alias("csat_max_burst_id"),
    )
)

result = (
    sequenced
    .filter(F.col("burst_id") < 5)
    .select(
        "id",
        "requester_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "burst_id",
        "requester_id_avg_4",
        "csat_max_burst_id",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)
