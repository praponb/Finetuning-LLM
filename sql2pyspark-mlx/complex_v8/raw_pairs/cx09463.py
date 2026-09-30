from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor")
w_all_by_days_since_event_at_d = Window.orderBy("days_since_event_at_distance", "id")
w_event_type_actor_or_unknown = Window.partitionBy("event_type", "actor_or_unknown").orderBy(F.col("days_since_event_at_distance").desc(), "id").rowsBetween(-3, Window.currentRow)
w_event_type_by_days_since_eve = Window.partitionBy("event_type").orderBy(F.col("days_since_event_at_minus_median").desc(), "id")

base_events = df_ticket_events

annotated = (
    base_events
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

actor_stats = (
    annotated
    .groupBy("actor")
    .agg(F.percentile_approx("days_since_event_at", 0.5).alias("median_days_since_event_at"))
)

vs_group = (
    annotated.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.days_since_event_at") >= F.col("g.median_days_since_event_at"))
    .select(
        "s.*",
        "g.median_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.median_days_since_event_at"), 2).alias("days_since_event_at_minus_median"),
    )
)

derived = (
    vs_group
    .filter(F.col("days_since_event_at_minus_median") > -560.0)
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_prefix",
        "event_at_label",
        "days_since_event_at_minus_median",
        F.abs(F.col("days_since_event_at") - 210).alias("days_since_event_at_distance"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

windowed = (
    derived
    .select(
        "*",
        F.round(F.col("days_since_event_at_distance") - F.avg("days_since_event_at_distance").over(w_actor), 2).alias("days_since_event_at_distance_vs_actor_avg"),
        F.row_number().over(w_all_by_days_since_event_at_d).alias("seq_no"),
        F.max("days_since_event_at_distance").over(w_event_type_actor_or_unknown).alias("days_since_event_at_distance_max_4"),
        F.size(F.collect_set("actor_or_unknown").over(w_actor)).alias("distinct_actor_or_unknown_in_actor"),
        F.lead("days_since_event_at_distance", 1, 0).over(w_all_by_days_since_event_at_d).alias("next_days_since_event_at_distance"),
        F.round(F.cume_dist().over(w_event_type_by_days_since_eve), 3).alias("days_since_event_at_minus_median_cume_dist"),
    )
)

result = (
    windowed
    .groupBy("actor_or_unknown")
    .pivot("event_type", ["resolved", "assigned", "escalated"])
    .agg(F.max("distinct_actor_or_unknown_in_actor").alias("max_v"), F.sum("distinct_actor_or_unknown_in_actor").alias("sum_v"))
    .orderBy("actor_or_unknown")
)
