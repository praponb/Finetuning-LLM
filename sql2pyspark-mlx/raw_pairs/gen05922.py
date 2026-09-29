from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sum(F.col("p.price")).alias("total_price"),
        F.round(F.avg(F.col("p.stock_qty")), 2).alias("avg_stock_qty"),
    )
    .orderBy(F.col("id").desc())
    .limit(3)
)
