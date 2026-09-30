from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.avg(F.col("p.stock_qty")).alias("avg_stock_qty"),
        F.round(F.avg(F.col("p.price")), 2).alias("avg_price"),
    )
    .filter(F.col("avg_price") > 68)
)
