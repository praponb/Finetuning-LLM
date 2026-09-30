from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("channel")
    .intersect(
        df_orders
        .filter(F.col("customer_id") == 21)
        .select("channel")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
        F.when(F.col("discount") >= 0.44, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
    )
    .orderBy(F.col("activity_month").desc())
)
