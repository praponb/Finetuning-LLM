from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("category").isin(["Home", "Sports", "Grocery"]) & (F.col("price") <= 1190))
)

products_stats = (
    recent_products
    .groupBy(
        F.when(F.col("stock_qty") >= 433, "high").when(F.col("stock_qty") >= 196, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
)

result = (
    products_stats
    .filter(F.col("pct_acme") >= 27)
    .select("stock_qty_bucket", "pct_acme")
    .orderBy(F.col("stock_qty_bucket").desc(), "pct_acme")
)
