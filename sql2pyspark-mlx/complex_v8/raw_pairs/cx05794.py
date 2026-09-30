from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_actor_by_event_at_week = Window.partitionBy("actor").orderBy(F.col("event_at_week").desc(), "id")
w_dedup = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("ticket_priority").orderBy(F.col("event_at").desc(), F.col("id").desc())

filtered_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

windowed = (
    filtered_events
    .withColumn("next3_event_at_week", F.lead("event_at_week", 3).over(w_all_by_event_at))
    .withColumn("next3_event_at", F.lead("event_at", 3).over(w_actor_by_event_at_week))
)

with_parent = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_prefix",
        "s.event_type_clean",
        "s.event_at_week",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

deduped = (
    with_parent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest_per_key = (
    deduped
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_prefix",
        "event_type_clean",
        "event_at_week",
        "ticket_priority",
        "ticket_csat",
        "ticket_channel",
        (F.col("event_at_week") + F.expr("INTERVAL 7 DAYS")).alias("event_at_week_plus_7d"),
    )
    .orderBy(F.col("id").desc(), F.col("ticket_csat").asc_nulls_last())
)
