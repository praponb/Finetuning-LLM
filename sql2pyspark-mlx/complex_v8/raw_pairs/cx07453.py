from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("requester_id_tier").orderBy("closed_at", "id", "event_id")

scoped = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        (F.col("closed_at") + F.expr("INTERVAL 2 HOURS")).alias("closed_at_plus_2h"),
        F.when(F.col("requester_id") >= 15, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

detail_rows = (
    scoped.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.requester_id_tier",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

deduped = (
    detail_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "requester_id_tier",
        "event_id",
        "event_type",
        "event_at",
        F.length(F.trim(F.col("requester_id_tier"))).alias("requester_id_tier_len"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)
