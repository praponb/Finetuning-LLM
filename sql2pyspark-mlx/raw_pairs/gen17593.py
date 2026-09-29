from pyspark.sql import functions as F

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity").isNotNull()), "left_semi")
)

result = (
    eligible
    .withColumn("year_start", F.date_trunc("YEAR", "launch_date"))
    .withColumn("price_tier", F.when(F.col("price") >= 1240, "PREMIUM").otherwise("STANDARD"))
    .cube("year_start", "brand", "price_tier")
    .agg(
        F.max(F.when(F.col("category") == "Sports", F.col("price"))).alias("sports_max_price"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("stock_qty_per_grocery"),
        F.grouping("year_start").alias("grouping_id"),
    )
    .orderBy(F.col("stock_qty_per_grocery").asc_nulls_last())
)
