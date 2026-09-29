from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")

base_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("ticket_id") <= 56))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
    )
)

windowed = (
    base_events
    .select(
        "*",
        F.round((F.unix_timestamp("event_at_plus_7d") - F.unix_timestamp(F.lag("event_at_plus_7d").over(w_ticket_id_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_plus_7d"),
        F.dense_rank().over(w_actor_by_event_at_ties).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

derived = (
    windowed
    .filter(F.col("minutes_since_prev_event_at_plus_7d") < 3750.0)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_plus_7d",
        F.substring(F.trim(F.col("event_at_label")), 1, 2).alias("event_at_label_prefix"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    derived.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    with_counts
    .rollup("ticket_id", "actor", "event_count")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("event_at").alias("last_event_at"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "event_count",
        "grouping_level",
        "last_event_at",
    )
)
