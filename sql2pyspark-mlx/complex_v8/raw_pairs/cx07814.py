from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_ticket_id_by_days_since_even = Window.partitionBy("ticket_id").orderBy("days_since_event_at", "id")
w_event_at_month_by_days_since = Window.partitionBy(F.month("event_at")).orderBy("days_since_event_at", "id")
w_ticket_id_by_days_since_even_2 = Window.partitionBy("ticket_id").orderBy("days_since_event_at")
w_ticket_id_by_days_since_even_3 = Window.partitionBy("ticket_id").orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.size(F.collect_set("actor").over(w_event_type)).alias("distinct_actor_in_event_type"),
        F.ntile(5).over(w_ticket_id_by_days_since_even).alias("days_since_event_at_quintile"),
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_event_type), 2).alias("days_since_event_at_vs_event_type_avg"),
        F.ntile(4).over(w_event_at_month_by_days_since).alias("days_since_event_at_quartile"),
        F.dense_rank().over(w_ticket_id_by_days_since_even_2).alias("days_since_event_at_dense_rank"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_days_since_even_3).alias("second_event_at"),
    )
)

screened = (
    with_trends.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

actor_stats = (
    screened
    .groupBy("actor")
    .agg(F.avg("days_since_event_at_vs_event_type_avg").alias("avg_days_since_event_at_vs_event_type_avg"))
)

above_average = (
    screened.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.days_since_event_at_vs_event_type_avg") > F.col("g.avg_days_since_event_at_vs_event_type_avg"))
    .select(
        "s.*",
        "g.avg_days_since_event_at_vs_event_type_avg",
        F.round(F.col("s.days_since_event_at_vs_event_type_avg") - F.col("g.avg_days_since_event_at_vs_event_type_avg"), 2).alias("days_since_event_at_vs_event_type_avg_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["distinct_actor_in_event_type", "days_since_event_at_quintile", "days_since_event_at_quartile"], ["distinct_actor_in_event_type"], ["days_since_event_at_quintile"], []],
        "distinct_actor_in_event_type", "days_since_event_at_quintile", "days_since_event_at_quartile",
    )
    .agg(
        F.round(F.avg("days_since_event_at_vs_event_type_avg_minus_avg"), 2).alias("avg_days_since_event_at_vs_event_type_avg_minus_"),
        F.max("days_since_event_at_vs_event_type_avg_minus_avg").alias("max_days_since_event_at_vs_event_type_avg_minus_"),
        F.round(F.avg("days_since_event_at"), 2).alias("avg_days_since_event_at"),
    )
)
