from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type").orderBy("event_at_date", "id")
w_dedup_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())

filtered = (
    df_ticket_events
    .filter((F.col("actor") != "system") & (~F.col("event_type").isin("escalated", "assigned")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined_ticket = (
    latest.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

derived = (
    joined_ticket
    .select(
        "*",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix_2"),
        F.round((F.unix_timestamp(F.col("ticket_closed_at")) - F.unix_timestamp(F.col("event_at"))) / 60, 1).alias("minutes_event_at_to_ticket_closed_at"),
    )
)

deduped = (
    derived
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select("event_type", "actor_prefix_2", "minutes_event_at_to_ticket_closed_at")
    .distinct()
    .orderBy("actor_prefix_2", "event_type")
)
