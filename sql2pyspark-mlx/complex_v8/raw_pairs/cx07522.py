from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_event_type = Window.partitionBy(F.month("event_at"), "event_type")
w_ticket_id_event_actor_count = Window.partitionBy("ticket_id", "event_actor_count").orderBy(F.col("event_actor_count").desc(), "id").rowsBetween(-2, -1)

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor") != "customer") & (F.col("event_type") != "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

without_matches = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel") == "chat") & (F.col("t.opened_at") < F.lit("2025-02-24 15:00:00").cast("timestamp")), "left_semi")
)

derived = (
    without_matches
    .filter(F.col("ticket_id").isNotNull())
    .withColumn("days_since_event_at", F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at",
    )
)

with_windows = (
    summarized_rows
    .select(
        "*",
        F.round(F.col("event_count") / F.nullif(F.sum("event_count").over(w_event_at_month_event_type), F.lit(0)), 4).alias("event_count_share_of_event_at_month_event_type"),
        F.max("days_since_event_at").over(w_ticket_id_event_actor_count).alias("days_since_event_at_max_2_prior"),
    )
)

result = (
    with_windows
    .select("days_since_event_at_max_2_prior", "ticket_id", "event_at")
    .distinct()
    .orderBy(F.col("ticket_id").desc_nulls_first())
)
