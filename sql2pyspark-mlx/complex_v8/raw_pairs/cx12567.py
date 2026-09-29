from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy(F.col("last_event_at_2").desc(), F.col("id").desc(), F.col("event_id").desc())

prepared_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

tagged = (
    prepared_events
    .filter(F.col("event_type_ref").isNotNull())
    .withColumn("event_at_quarter", F.quarter(F.col("event_at")))
)

detail_rows_events = (
    tagged.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_at_quarter",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

summarized_rows = (
    detail_rows_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

unique_rows = (
    summarized_rows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "ticket_id",
        "event_at_quarter",
        "event_id",
        "event_actor",
        "event_event_type",
        "event_event_at",
        "last_event_at",
        "last_event_at_2",
        F.coalesce(F.col("event_at_quarter"), F.lit(0)).alias("event_at_quarter_filled"),
    )
    .orderBy("event_id")
)
