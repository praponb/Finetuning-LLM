from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_days_since_eve = Window.partitionBy("event_type").orderBy(F.col("days_since_event_at_minus_avg").desc(), "id").rowsBetween(-1, -1)
w_event_type_by_days_since_eve_2 = Window.partitionBy("event_type").orderBy(F.col("days_since_event_at_minus_avg").desc(), "id")
w_event_type_by_days_since_eve_3 = Window.partitionBy("event_type").orderBy(F.col("days_since_event_at_minus_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_year = Window.partitionBy(F.year("event_at"))
w_actor_group = Window.partitionBy("actor_group")
w_actor_group_by_days_since_ev = Window.partitionBy("actor_group").orderBy("days_since_event_at_minus_avg")
w_all_by_avg_days_since_event = Window.orderBy("avg_days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

scored = (
    filtered_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "event_at",
        "event_type",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_or_unknown")), " ", "_")).alias("event_type_or_unknown_slug"),
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

event_type_stats = (
    scored
    .groupBy("event_type")
    .agg(F.avg("days_since_event_at").alias("avg_days_since_event_at"))
)

vs_group = (
    scored.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.days_since_event_at") > F.col("g.avg_days_since_event_at"))
    .select(
        "s.*",
        "g.avg_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.avg_days_since_event_at"), 2).alias("days_since_event_at_minus_avg"),
    )
)

with_activity = (
    vs_group
    .crossJoin(vs_group.agg(F.avg("days_since_event_at_minus_avg").alias("overall_avg_days_since_event_at_minus_avg")))
    .filter(F.col("days_since_event_at_minus_avg") > F.col("overall_avg_days_since_event_at_minus_avg"))
    .drop("overall_avg_days_since_event_at_minus_avg")
)

derived = (
    with_activity
    .withColumn("days_since_event_at_minus_avg_days_since_event_a", F.col("days_since_event_at") - F.col("avg_days_since_event_at"))
)

metrics = (
    derived
    .select(
        "id",
        "event_at",
        "event_type_or_unknown_slug",
        "actor_group",
        "days_since_event_at",
        "avg_days_since_event_at",
        "days_since_event_at_minus_avg",
        F.sum("avg_days_since_event_at").over(w_event_type_by_days_since_eve).alias("avg_days_since_event_at_sum_1_prior"),
        F.round(F.cume_dist().over(w_event_type_by_days_since_eve_2), 3).alias("days_since_event_at_minus_avg_cume_dist"),
        F.first("days_since_event_at", ignorenulls=True).over(w_event_type_by_days_since_eve_3).alias("first_known_days_since_event_at"),
        F.count(F.lit(1)).over(w_event_at_year).alias("rows_in_event_at_year"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "event_at",
        "event_type_or_unknown_slug",
        "actor_group",
        "days_since_event_at",
        "avg_days_since_event_at",
        "days_since_event_at_minus_avg",
        "days_since_event_at_minus_avg_cume_dist",
        "first_known_days_since_event_at",
        "rows_in_event_at_year",
        F.count(F.lit(1)).over(w_actor_group).alias("rows_in_actor_group"),
        F.round(F.percent_rank().over(w_actor_group_by_days_since_ev), 3).alias("days_since_event_at_minus_avg_percent_rank"),
        F.round(F.avg("avg_days_since_event_at").over(w_actor_group), 2).alias("actor_group_avg_avg_days_since_event_at"),
        F.nth_value("days_since_event_at", 2).over(w_all_by_avg_days_since_event).alias("second_days_since_event_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "event_at",
        "event_type_or_unknown_slug",
        "actor_group",
        "days_since_event_at",
        "avg_days_since_event_at",
        "days_since_event_at_minus_avg",
        "days_since_event_at_minus_avg_cume_dist",
        "first_known_days_since_event_at",
        "rows_in_event_at_year",
        "rows_in_actor_group",
        "days_since_event_at_minus_avg_percent_rank",
        "actor_group_avg_avg_days_since_event_at",
        "second_days_since_event_at",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
    .orderBy(F.col("days_since_event_at_minus_avg").desc())
)
