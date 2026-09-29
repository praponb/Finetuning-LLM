from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_csat_running = Window.partitionBy("channel").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_channel_by_requester_id = Window.partitionBy("csat", "channel").orderBy(F.col("requester_id").desc(), "id")

recent = (
    df_tickets
    .filter(~F.col("priority").isin("P4", "P3"))
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
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

annotated = (
    recent
    .select(
        "*",
        F.least(F.col("requester_id"), F.col("csat_plus_requester_id")).alias("least_requester_id_csat_plus_requester_id"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.count(F.lit(1)).over(w_channel_by_csat_running).alias("running_count"),
        F.dense_rank().over(w_csat_channel_by_requester_id).alias("requester_id_dense_rank"),
    )
)

annotated_2 = (
    ranked
    .filter(F.col("least_requester_id_csat_plus_requester_id").between(10, 21))
    .withColumn("priority_len_plus_requester_id", F.col("priority_len") + F.col("requester_id"))
)

scored = (
    annotated_2
    .filter(F.col("running_count") <= 14)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "csat_plus_requester_id",
        "least_requester_id_csat_plus_requester_id",
        "priority_len",
        F.least(F.col("priority_len"), F.col("csat_plus_requester_id")).alias("least_priority_len_csat_plus_requester_id"),
    )
)

result = (
    scored
    .groupBy("agent_id", "priority_len")
    .pivot("priority", ["P4", "P2"])
    .agg(F.min("least_priority_len_csat_plus_requester_id"))
    .orderBy("agent_id", "priority_len")
)
