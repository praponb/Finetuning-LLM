from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_ticket_id = Window.partitionBy("ticket_id")
w_event_at_to_date_event_at_da = Window.partitionBy(F.to_date("event_at"), "event_at_dayofweek").orderBy("event_at_dayofweek", "id")

scoped_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date")) | (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

latest = (
    scoped_events
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

labeled = (
    latest
    .filter(F.col("actor_slug").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

combined_details = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.closed_at") >= F.lit("2025-01-15 08:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_prefix",
        "s.event_at_dayofweek",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.round(F.col("event_at_dayofweek") - F.avg("event_at_dayofweek").over(w_ticket_id), 2).alias("event_at_dayofweek_vs_ticket_id_avg"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_event_at_to_date_event_at_da))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.lag("ticket_closed_at", 3).over(w_event_at_to_date_event_at_da).alias("prev3_ticket_closed_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_prefix",
        "event_at_dayofweek",
        "ticket_priority",
        "ticket_closed_at",
        "minutes_since_prev_ticket_closed_at",
        F.concat(F.col("actor_prefix"), F.lit("#"), F.col("id").cast("string")).alias("actor_prefix_ref"),
    )
    .orderBy(F.col("event_at_dayofweek").desc(), "id")
)
