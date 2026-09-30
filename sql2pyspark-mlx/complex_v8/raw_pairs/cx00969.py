from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_to_date_by_days_sin = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("days_since_event_at").desc())
w_actor_group = Window.partitionBy("actor_group")
w_actor_by_event_at_prior_3 = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(-3, -1)
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_event_type_by_days_sin = Window.partitionBy("actor", "event_type").orderBy("days_since_event_at", "id")
w_actor_event_type_by_days_sin_2 = Window.partitionBy("actor", "event_type").orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date_by_days_sin_2 = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("days_since_event_at").desc(), "id")

cleaned = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.rank().over(w_event_at_to_date_by_days_sin).alias("days_since_event_at_rank"),
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_actor_group), 2).alias("days_since_event_at_vs_actor_group_avg"),
        F.round(F.avg("days_since_event_at").over(w_actor_by_event_at_prior_3), 2).alias("days_since_event_at_avg_3_prior"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_actor_event_type_by_days_sin), 3).alias("days_since_event_at_percent_rank"),
        F.last("days_since_event_at", ignorenulls=True).over(w_actor_event_type_by_days_sin_2).alias("last_known_days_since_event_at"),
        F.round(F.cume_dist().over(w_event_at_to_date_by_days_sin), 3).alias("days_since_event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_to_date_by_days_sin_2))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.round(F.avg("days_since_event_at").over(w_actor_group), 2).alias("actor_group_avg_days_since_event_at"),
    )
)

first_rows = (
    with_trends
    .filter(F.col("days_since_event_at_rank") <= 5)
    .drop("days_since_event_at_rank")
)

per_group = (
    first_rows
    .filter(F.col("days_since_event_at_avg_3_prior").between(245.0, 665.0))
    .groupBy(
        F.when(F.col("days_since_event_at") >= 595, "HIGH").when(F.col("days_since_event_at") >= 245, "MEDIUM").otherwise("LOW").alias("days_since_event_at_tier"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "assigned", 1).otherwise(0)).alias("n_event_type_assigned"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    per_group
    .filter(F.col("n_rows").between(3, 9))
    .select(
        "days_since_event_at_tier",
        "n_event_type_assigned",
        "n_rows",
        F.initcap(F.trim(F.col("days_since_event_at_tier"))).alias("days_since_event_at_tier_clean"),
    )
    .orderBy("n_rows", F.col("days_since_event_at_tier_clean").desc())
)
