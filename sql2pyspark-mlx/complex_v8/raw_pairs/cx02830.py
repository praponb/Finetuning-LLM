from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id").orderBy("event_count", "id")

scoped_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(~F.col("actor").isin("agent", "customer"), 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_counts = (
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at",
    )
)

latest = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy("ticket_id", "event_at")
    .pivot("actor", ["agent", "customer"])
    .agg(F.count("event_count"))
    .orderBy("ticket_id", "event_at")
)
