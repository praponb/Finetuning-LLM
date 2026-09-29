from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("price") < 620)
    .groupBy("supplier_id")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.min("price").alias("min_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.min_price") >= 510)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("pbs.total_stock_qty"),
        F.col("pbs.min_price"),
    )
    .orderBy("id")
)
