from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_actor_count_by_f = Window.partitionBy("actor", "event_actor_count").orderBy(F.col("first_event_at_2").asc_nulls_last(), "id")
w_ticket_id = Window.partitionBy("ticket_id")

recent = (
    df_ticket_events
    .filter((F.col("event_type") != "reopened") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(~F.col("actor").isin("system", "customer"), 1)).alias("event_actor_count"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_counts = (
    recent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.first_event_at_2",
    )
)

metrics = (
    with_counts
    .select(
        "*",
        F.round((F.unix_timestamp("first_event_at") - F.unix_timestamp(F.lag("first_event_at").over(w_actor_event_actor_count_by_f))) / 60, 1).alias("minutes_since_prev_first_event_at"),
        F.max("event_actor_count").over(w_ticket_id).alias("ticket_id_max_event_actor_count"),
        F.round(F.col("event_actor_count") - F.avg("event_actor_count").over(w_ticket_id), 2).alias("event_actor_count_vs_ticket_id_avg"),
    )
)

result = (
    metrics
    .groupingSets(
        [["event_actor_count", "actor"], ["event_actor_count"]],
        "event_actor_count", "actor",
    )
    .agg(
        F.min("ticket_id_max_event_actor_count").alias("min_ticket_id_max_event_actor_count"),
        F.round(F.stddev("ticket_id_max_event_actor_count"), 2).alias("stddev_ticket_id_max_event_actor_count"),
        F.sum("event_actor_count_vs_ticket_id_avg").alias("total_event_actor_count_vs_ticket_id_avg"),
    )
    .orderBy(
        F.col("event_actor_count").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
