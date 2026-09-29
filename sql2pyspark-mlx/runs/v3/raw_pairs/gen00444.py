from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.round(F.avg(F.col("p.stock_qty")), 2).alias("avg_stock_qty"),
        (F.max(F.col("p.stock_qty")) - F.min(F.col("p.stock_qty"))).alias("stock_qty_range"),
    )
    .filter(F.col("product_count") > 1)
    .orderBy(F.col("stock_qty_range").desc(), "product_count")
)
