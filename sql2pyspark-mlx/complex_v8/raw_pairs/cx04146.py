from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_requester_id = Window.partitionBy("csat").orderBy(F.col("requester_id").desc(), "id")

cleaned = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "bug")) | (F.hour(F.col("closed_at")).between(6, 10)))
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
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.round(F.percent_rank().over(w_csat_by_requester_id), 3).alias("requester_id_percent_rank"),
        (F.col("priority_len") - F.lag("priority_len").over(w_csat_by_requester_id)).alias("priority_len_change"),
    )
)

exploded = (
    ranked
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "priority_len",
        "channel_or_unknown",
        "priority_clean",
        "requester_id_percent_rank",
        "priority_len_change",
        F.explode("tags").alias("tag"),
    )
)

tagged = (
    exploded
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "priority_len",
        "channel_or_unknown",
        "priority_clean",
        "requester_id_percent_rank",
        "tag",
        F.when(F.col("requester_id") >= 17, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
        (F.col("closed_at") + F.expr("INTERVAL 2 HOURS")).alias("closed_at_plus_2h"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.min("requester_id").alias("min_ticket_requester_id"))
)

with_counts = (
    tagged.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.min_ticket_requester_id")
)

result = (
    with_counts
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "priority_len",
        "priority_clean",
        "closed_at_plus_2h",
        "min_ticket_requester_id",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
    .orderBy(F.col("agent_id").desc_nulls_first())
)
