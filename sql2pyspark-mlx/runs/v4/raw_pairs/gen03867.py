from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") != "UK")
    .groupBy(
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.col("p.stock_qty")).alias("stock_qty_count"),
        F.sum(F.col("p.stock_qty")).alias("total_stock_qty"),
    )
    .orderBy("stock_qty_count")
)
