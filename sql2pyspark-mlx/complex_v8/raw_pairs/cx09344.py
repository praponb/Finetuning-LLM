from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_closed_at_movi = Window.partitionBy("event_type").orderBy(F.col("closed_at").asc_nulls_last(), "id", "event_id").rowsBetween(-3, Window.currentRow)
w_event_type_by_closed_at_runn = Window.partitionBy("event_type").orderBy(F.col("closed_at").asc_nulls_last(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = df_tickets.filter(F.col("closed_at") >= F.lit("2025-01-24 01:00:00").cast("timestamp"))

exploded_detail_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

metrics = (
    exploded_detail_events
    .select(
        "*",
        F.round(F.avg("csat").over(w_event_type_by_closed_at_movi), 2).alias("csat_avg_4"),
        F.sum("csat").over(w_event_type_by_closed_at_runn).alias("running_csat"),
        F.min("csat").over(w_event_type_by_closed_at_runn).alias("min_csat_so_far"),
    )
)

result = (
    metrics
    .select(
        "id",
        "csat",
        "event_id",
        "event_at",
        "event_type",
        "running_csat",
        "min_csat_so_far",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)
