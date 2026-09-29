from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy("hours_since_open", "id")

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

without_matches = (
    in_window.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")), "left_semi")
)

unique_rows = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "event_type",
        "hours_since_open",
        F.when(F.col("hours_since_open") >= 43.2, "large").when(F.col("hours_since_open") >= 18.0, "medium").otherwise("small").alias("hours_since_open_tier"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("agent_id").cast("string")).alias("event_type_ref"),
    )
    .orderBy(F.col("event_type_ref").desc(), F.col("event_type").desc())
)
