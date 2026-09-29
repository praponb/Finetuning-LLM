from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("price_tier", F.when(F.col("price") >= 1080, "LARGE").otherwise("SMALL"))
    .groupingSets([["price_tier", "brand"], ["price_tier"]], "price_tier", "brand")
    .agg(
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
    )
    .orderBy(F.col("brand").desc())
)
