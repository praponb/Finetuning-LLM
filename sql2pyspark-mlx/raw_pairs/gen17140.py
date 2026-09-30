from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .withColumn("week_start", F.date_trunc("WEEK", "launch_date"))
    .withColumn("price_tier", F.when(F.col("price") >= 1130, "HIGH").otherwise("LOW"))
    .rollup("week_start", "price_tier")
    .agg(
        F.max(F.when(F.col("category") == "Fashion", F.col("price"))).alias("fashion_max_price"),
        F.corr("cost", "price").alias("corr_cost_price"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.corr("cost", "price").alias("corr_cost_price_2"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.grouping("week_start").alias("grouping_id"),
    )
)
