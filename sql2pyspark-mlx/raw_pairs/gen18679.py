from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity").between(5, 6)), "left_semi")
)

result = (
    kept
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 42, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .groupingSets(
        [["shipping_fee_tier", "week_start"], ["shipping_fee_tier"]],
        "shipping_fee_tier",
        "week_start",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("shipping_fee")).otherwise(0)).alias("partner_shipping_fee"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("discount").alias("min_discount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
