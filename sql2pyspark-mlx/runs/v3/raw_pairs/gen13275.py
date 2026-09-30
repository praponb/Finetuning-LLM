from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost").between(250, 540))
    .groupBy("supplier_id")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.count("price").alias("price_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("pbs.min_stock_qty"),
        F.col("pbs.price_count"),
    )
)
