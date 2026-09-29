from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_priority_by_csat_ties = Window.partitionBy("priority").orderBy("csat")
w_priority_by_csat = Window.partitionBy("priority").orderBy("csat", "id")
w_priority_by_csat_moving_1 = Window.partitionBy("priority").orderBy("csat", "id").rowsBetween(-1, Window.currentRow)

base_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) & (F.col("agent_id") >= 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.when(F.col("csat") >= 4, "HIGH").when(F.col("csat") >= 3, "MEDIUM").otherwise("LOW").alias("csat_tier"),
    )
)

scored = (
    base_tickets
    .select(
        "*",
        F.concat(F.col("csat_tier"), F.lit("#"), F.col("agent_id").cast("string")).alias("csat_tier_ref"),
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_priority), F.lit(0)), 4).alias("csat_share_of_agent_id_priority"),
        F.round(F.percent_rank().over(w_priority_by_csat_ties), 3).alias("csat_percent_rank"),
        F.row_number().over(w_priority_by_csat).alias("seq_no"),
        (F.col("requester_id") - F.lag("requester_id").over(w_priority_by_csat)).alias("requester_id_change"),
        F.round(F.avg("requester_id").over(w_priority_by_csat_moving_1), 2).alias("requester_id_avg_2"),
        F.lead("requester_id", 2).over(w_priority_by_csat).alias("next2_requester_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "priority",
        "csat",
        "tags",
        "channel_or_unknown",
        "csat_tier",
        "csat_tier_ref",
        "priority_prefix",
        "csat_percent_rank",
        "seq_no",
        "requester_id_change",
        "requester_id_avg_2",
        "next2_requester_id",
        (F.col("requester_id") + F.col("requester_id_avg_2")).alias("requester_id_plus_requester_id_avg_2"),
        F.least(F.col("requester_id_change"), F.col("next2_requester_id")).alias("least_requester_id_change_next2_requester_id"),
    )
    .orderBy(F.col("id").desc(), F.col("requester_id").desc())
)
