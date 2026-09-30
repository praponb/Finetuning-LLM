from pyspark.sql import functions as F

with_dims = (
    df_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.channel").isin(["store", "partner", "web"]))
    .select("s.*", F.col("p.discount"), F.col("p.amount").alias("order_amount"))
)

result = (
    with_dims
    .withColumn("amount_tier", F.when(F.col("amount") >= 1690, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["amount_tier", "method"], ["amount_tier"], []],
        "amount_tier",
        "method",
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.max(F.when(F.col("method") == "cash", F.col("discount"))).alias("cash_max_discount"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
    .orderBy(F.col("avg_amount").desc_nulls_first())
)
