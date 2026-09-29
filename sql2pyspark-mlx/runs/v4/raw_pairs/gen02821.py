from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("products_by_supplier.min_stock_qty"),
    )
    .orderBy(F.col("country").desc())
)
