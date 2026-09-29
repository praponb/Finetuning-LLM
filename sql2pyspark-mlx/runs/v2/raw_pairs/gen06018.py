from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("pbs.total_stock_qty"),
        F.col("pbs.avg_price"),
    )
)
