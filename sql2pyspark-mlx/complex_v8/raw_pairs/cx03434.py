from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_opened_at_date_mo = Window.partitionBy("channel").orderBy(F.col("opened_at_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")
w_dedup = Window.partitionBy("channel", "event_actor").orderBy("csat_per_requester_id", "id", "event_id")

valid = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-19 10:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.avg("requester_id").over(w_channel_by_opened_at_date_mo), 2).alias("requester_id_avg_4"),
        F.max("requester_id").over(w_agent_id).alias("agent_id_max_requester_id"),
    )
)

detail_rows = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.requester_id_filled",
        "s.csat_per_requester_id",
        "s.requester_id_avg_4",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

unique_rows = (
    detail_rows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

qualified = (
    unique_rows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("csat_per_requester_id") >= 3.0)
)

result = (
    qualified
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        "requester_id_filled",
        "csat_per_requester_id",
        "event_id",
        "event_at",
        "event_type",
        "event_actor",
    )
    .orderBy(
        F.col("channel").desc(),
        F.col("requester_id_filled").desc(),
        "id",
        "event_id",
    )
    .limit(10)
)
