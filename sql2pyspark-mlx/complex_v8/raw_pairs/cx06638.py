from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())

recent = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor_prefix",
        "s.days_since_event_at",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

latest = (
    combined_details
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .filter(F.hour(F.col("ticket_closed_at")).between(9, 14))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor_prefix",
        "days_since_event_at",
        F.concat(F.col("actor_prefix"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_prefix_ref"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("ticket_id").desc())
)
