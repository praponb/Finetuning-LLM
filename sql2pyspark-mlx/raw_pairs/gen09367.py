from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("days_since_paid_at").desc())

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-04-23 13:00:00"))) & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.when(F.col("method").isin(["card", "bank_transfer"]), "Tier 1").otherwise("Other").alias("method_group"),
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)

enriched = (
    cleaned
    .filter(F.col("days_since_paid_at").between(41, 77))
    .select(
        "id",
        "order_id",
        "paid_at",
        "method_group",
        "days_since_paid_at",
        F.sum("days_since_paid_at").over(w).alias("running_sum_days_since_paid_at"),
        F.min("days_since_paid_at").over(w).alias("running_min_days_since_paid_at"),
        F.avg("days_since_paid_at").over(w).alias("running_avg_days_since_paid_at"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "order_id",
        "days_since_paid_at",
        "running_sum_days_since_paid_at",
        "running_avg_days_since_paid_at",
    )
)

joined = (
    deduped
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.days_since_paid_at",
        F.col("d.amount"),
        F.col("d.order_date"),
    )
)

result = (
    joined
    .groupBy("order_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("days_since_paid_at", [0.25, 0.5, 0.75]).alias("days_since_paid_at_quartiles"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .orderBy(F.col("amount_stddev").asc_nulls_last())
)
