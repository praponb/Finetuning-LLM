from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_event_type_by_event_at_2 = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter((F.col("event_type") == "reopened") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    filtered_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at", 2).over(w_event_type_actor_by_event_at).alias("prev2_event_at"),
        F.ntile(4).over(w_event_type_by_event_at).alias("event_at_quartile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("last_known_event_at"),
    )
)

actor_stats = (
    sequenced
    .groupBy("actor")
    .agg(F.avg("minutes_since_prev_event_at_2").alias("avg_minutes_since_prev_event_at_2"))
)

above_average = (
    sequenced.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.minutes_since_prev_event_at_2") > F.col("g.avg_minutes_since_prev_event_at_2"))
    .select(
        "s.*",
        "g.avg_minutes_since_prev_event_at_2",
        F.round(F.col("s.minutes_since_prev_event_at_2") - F.col("g.avg_minutes_since_prev_event_at_2"), 2).alias("minutes_since_prev_event_at_2_minus_avg"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

profiled = (
    above_average.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["actor", "ticket_id", "event_count"], ["actor"], ["ticket_id"]],
        "actor", "ticket_id", "event_count",
    )
    .agg(
        F.grouping("actor").alias("is_total_actor"),
        F.sum(F.when(F.col("event_type") == "resolved", 1).otherwise(0)).alias("n_event_type_resolved"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "ticket_id",
        "event_count",
        "is_total_actor",
        "n_event_type_resolved",
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_count").asc_nulls_last(),
    )
)
