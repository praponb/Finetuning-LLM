from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hours_since_open_tier_by_min = Window.partitionBy("hours_since_open_tier").orderBy("min_hours_since_open", "hours_since_open_tier")
w_hours_since_open_tier_by_min_2 = Window.partitionBy("hours_since_open_tier").orderBy(F.col("min_hours_since_open").desc(), "hours_since_open_tier")
w_hours_since_open_tier = Window.partitionBy("hours_since_open_tier")
w_hours_since_open_tier_by_min_3 = Window.partitionBy("hours_since_open_tier").orderBy(F.col("min_hours_since_open").desc(), "hours_since_open_tier").rowsBetween(-1, -1)
w_hours_since_open_tier_by_min_4 = Window.partitionBy("hours_since_open_tier").orderBy(F.col("min_hours_since_open").desc(), "hours_since_open_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

rollup_base = (
    matched
    .groupBy(
        F.when(F.col("hours_since_open") >= 39.6, "large").when(F.col("hours_since_open") >= 14.4, "medium").otherwise("small").alias("hours_since_open_tier"),
    )
    .agg(
        F.round(F.stddev("hours_since_open"), 2).alias("stddev_hours_since_open"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.min("hours_since_open").alias("min_hours_since_open"),
        F.sum("hours_since_open").alias("sum_hours_since_open_check"),
    )
    .filter(F.col("sum_hours_since_open_check") > 10.8)
    .drop("sum_hours_since_open_check")
)

with_trends = (
    rollup_base
    .select(
        "hours_since_open_tier",
        "priority_list",
        "min_hours_since_open",
        (F.col("stddev_hours_since_open") - F.lag("stddev_hours_since_open").over(w_hours_since_open_tier_by_min)).alias("stddev_hours_since_open_change"),
        (F.col("min_hours_since_open") - F.lag("min_hours_since_open").over(w_hours_since_open_tier_by_min_2)).alias("min_hours_since_open_change"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.ntile(4).over(w_hours_since_open_tier_by_min_2).alias("min_hours_since_open_quartile"),
        F.max("min_hours_since_open_change").over(w_hours_since_open_tier).alias("hours_since_open_tier_max_min_hours_since_open_c"),
        (F.col("min_hours_since_open") - F.lag("min_hours_since_open").over(w_hours_since_open_tier_by_min)).alias("min_hours_since_open_change_2"),
        F.round(F.avg("min_hours_since_open_change").over(w_hours_since_open_tier_by_min_3), 2).alias("min_hours_since_open_change_avg_1_prior"),
        F.first("min_hours_since_open_change").over(w_hours_since_open_tier_by_min_4).alias("first_min_hours_since_open_change"),
    )
)

result = (
    metrics
    .groupBy("min_hours_since_open_quartile")
    .pivot("hours_since_open_tier", ["small", "medium", "large"])
    .agg(F.sum("min_hours_since_open_change"))
)
