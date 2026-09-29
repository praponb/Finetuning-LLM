from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id", "event_type").orderBy("days_since_event_at", "id")

cleaned = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(9, 15)), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.first_event_at")
)

latest = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .rollup("actor", "event_type")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(100 * F.count(F.when(F.col("event_type") == "replied", 1)) / F.count("*"), 1).alias("pct_event_type_replied"),
    )
)
