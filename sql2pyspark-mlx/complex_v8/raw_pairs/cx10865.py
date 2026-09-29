from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type").orderBy("last_event_at_2", "id")
w_ticket_id_by_last_event_at_t = Window.partitionBy("ticket_id").orderBy("last_event_at")
w_last_event_at_year_event_typ = Window.partitionBy(F.year("last_event_at"), "event_type").orderBy("event_at", "id")

prepared_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

with_stats = (
    prepared_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

unique_rows = (
    with_stats
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    unique_rows
    .select(
        "*",
        F.round(F.percent_rank().over(w_ticket_id_by_last_event_at_t), 3).alias("last_event_at_percent_rank"),
        F.rank().over(w_last_event_at_year_event_typ).alias("event_at_rank"),
    )
)

combined_details_ticket = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.to_date(F.col("t.opened_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.team").alias("agent_team"),
    )
)

scored = (
    combined_details_ticket
    .filter(F.to_date(F.col("last_event_at_2")) < F.lit("2025-02-17").cast("date"))
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("last_event_at_2")) - F.unix_timestamp(F.col("last_event_at"))) / 60, 1).alias("minutes_last_event_at_to_last_event_at_2"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

result = (
    scored
    .groupBy("actor_or_unknown")
    .pivot("event_type", ["assigned", "resolved", "reopened"])
    .agg(F.min("last_event_at_percent_rank").alias("min_v"), F.sum("last_event_at_percent_rank").alias("sum_v"))
    .orderBy("actor_or_unknown")
)
