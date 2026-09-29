from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_days_sin = Window.partitionBy("event_type", "actor").orderBy(F.col("days_since_event_at_minus_avg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_actor = Window.partitionBy("ticket_id", "actor")
w_event_type_actor_by_days_sin_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("days_since_event_at_minus_avg").asc_nulls_last(), "id").rowsBetween(-1, -1)

valid_events = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

actor_stats = (
    valid_events
    .groupBy("actor")
    .agg(F.avg("days_since_event_at").alias("avg_days_since_event_at"))
)

outliers = (
    valid_events.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.days_since_event_at") > F.col("g.avg_days_since_event_at"))
    .select(
        "s.*",
        "g.avg_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.avg_days_since_event_at"), 2).alias("days_since_event_at_minus_avg"),
    )
)

with_trends = (
    outliers
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "avg_days_since_event_at",
        "days_since_event_at_minus_avg",
        F.last("avg_days_since_event_at").over(w_event_type_actor_by_days_sin).alias("last_avg_days_since_event_at"),
        F.round(F.col("avg_days_since_event_at") / F.nullif(F.sum("avg_days_since_event_at").over(w_ticket_id_actor), F.lit(0)), 4).alias("avg_days_since_event_at_share_of_ticket_id_actor"),
        F.sum("days_since_event_at").over(w_event_type_actor_by_days_sin_2).alias("days_since_event_at_sum_1_prior"),
    )
)

derived = (
    with_trends
    .filter(F.col("last_avg_days_since_event_at") < 455.0)
    .withColumn("event_at_hour", F.hour(F.col("event_at")))
)

result = (
    derived
    .filter(F.col("avg_days_since_event_at_share_of_ticket_id_actor") > 0.2)
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "avg_days_since_event_at",
        "days_since_event_at_minus_avg",
        "last_avg_days_since_event_at",
        "avg_days_since_event_at_share_of_ticket_id_actor",
        "days_since_event_at_sum_1_prior",
        "event_at_hour",
    )
    .orderBy(F.col("days_since_event_at_minus_avg").desc(), "id")
    .limit(10)
)
