from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at")

prepared_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("ticket_id") <= 22))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

ranked = (
    prepared_events
    .select(
        "*",
        F.dense_rank().over(w_actor_event_type_by_event_at).alias("event_at_dense_rank"),
        F.ntile(5).over(w_actor_event_type_by_event_at).alias("event_at_quintile"),
        F.rank().over(w_actor_event_type_by_event_at_2).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_actor_event_type_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

profiled = (
    ranked.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    profiled
    .rollup("event_type", "event_count")
    .agg(
        F.grouping("event_type").alias("is_total_event_type"),
        F.round(F.avg("event_at_quintile"), 2).alias("avg_event_at_quintile"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "event_count",
        "is_total_event_type",
        "avg_event_at_quintile",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("event_count").asc_nulls_last(),
    )
)
