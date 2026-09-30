from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("supplier_id") == 16) | (F.col("cost") > 550))
    .withColumn("clean_category", F.trim("category"))
)

products_stats = (
    base
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
)

result = (
    products_stats
    .filter(F.col("pct_acme") > 58)
    .select("supplier_id", "avg_stock_qty", "pct_acme")
    .orderBy("supplier_id", "avg_stock_qty")
)
