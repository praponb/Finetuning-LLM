from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.avg_stock_qty") > 30)
    .select(F.col("s.city"), F.col("t.avg_stock_qty"), F.col("t.stock_qty_count"))
)
