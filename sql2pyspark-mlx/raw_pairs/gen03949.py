from pyspark.sql import functions as F

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price") >= 330), "left_anti")
)

result = (
    kept
    .withColumn("price_tier", F.when(F.col("price") >= 1190, "HIGH").otherwise("LOW"))
    .rollup("price_tier", "brand")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("price_tier").desc())
)
