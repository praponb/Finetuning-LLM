from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id", "event_type").orderBy("last_event_at", "id")
w_all_by_event_event_at_runnin = Window.orderBy("event_event_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_event_at = Window.orderBy("event_event_at", "id", "event_id")
w_all_by_event_event_at_ties = Window.orderBy("event_event_at")
w_all_by_event_event_at_full = Window.orderBy("event_event_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at_2",
    )
)

deduped = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

line_level = (
    deduped.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.last_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

with_windows = (
    line_level
    .select(
        "id",
        "event_id",
        "event_event_type",
        F.first("last_event_at", ignorenulls=True).over(w_all_by_event_event_at_runnin).alias("first_known_last_event_at"),
        F.ntile(3).over(w_all_by_event_event_at).alias("event_event_at_tercile"),
        F.round(F.percent_rank().over(w_all_by_event_event_at), 3).alias("event_event_at_percent_rank"),
        F.last("last_event_at", ignorenulls=True).over(w_all_by_event_event_at_runnin).alias("last_known_last_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_event_at_ties), 3).alias("event_event_at_percent_rank_2"),
        F.first("event_event_at").over(w_all_by_event_event_at_full).alias("first_event_event_at"),
    )
)

with_details_event = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.id") == F.col("s.event_id")) & (F.col("t.event_type") != "assigned"), "left")
    .select(
        "s.id",
        "s.event_id",
        "s.first_known_last_event_at",
        "s.event_event_at_tercile",
        "s.event_event_at_percent_rank",
        "s.event_event_at_percent_rank_2",
        "s.first_event_event_at",
        F.col("t.ticket_id").alias("event_ticket_id"),
        "t.event_at",
        "t.event_type",
    )
)

result = (
    with_details_event
    .select(
        "id",
        "event_id",
        "first_known_last_event_at",
        "event_event_at_tercile",
        "event_event_at_percent_rank_2",
        "first_event_event_at",
        "event_ticket_id",
        "event_at",
        F.coalesce(F.col("event_event_at_tercile"), F.lit(0)).alias("event_event_at_tercile_filled"),
    )
    .orderBy(F.col("event_at").desc(), "id")
)
