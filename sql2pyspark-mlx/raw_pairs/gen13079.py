from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([20, 29]))
        .select("id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quantity_tier", "order_id"], ["quantity_tier"]],
        "quantity_tier",
        "order_id",
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("order_id").desc_nulls_first())
)
