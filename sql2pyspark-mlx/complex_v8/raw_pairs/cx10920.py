from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("ticket_id") >= 50))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.row_number().over(w_actor_by_event_at).alias("event_at_seq"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.ntile(4).over(w_actor_by_event_at).alias("event_at_quartile"),
    )
)

top_per_group = (
    sequenced
    .filter((F.col("event_at_seq") <= 2) & (F.col("minutes_since_prev_event_at") <= 3500.0))
)

actor_stats = (
    top_per_group
    .groupBy("actor")
    .agg(F.avg("event_at_quartile").alias("avg_event_at_quartile"))
)

compared = (
    top_per_group.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_at_quartile") > F.col("g.avg_event_at_quartile"))
    .select(
        "s.*",
        "g.avg_event_at_quartile",
        F.round(F.col("s.event_at_quartile") - F.col("g.avg_event_at_quartile"), 2).alias("event_at_quartile_minus_avg"),
    )
)

result = (
    compared
    .groupBy("event_at_seq", "event_type")
    .pivot("actor", ["agent", "customer"])
    .agg(F.count("avg_event_at_quartile").alias("count_v"), F.sum("avg_event_at_quartile").alias("sum_v"))
    .orderBy("event_at_seq", "event_type")
)
