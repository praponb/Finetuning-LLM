from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("price").alias("unique_suppliers"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.rating"),
        F.col("s.country"),
        F.col("t.unique_suppliers"),
        F.col("t.max_stock_qty"),
    )
    .orderBy("rating", "unique_suppliers")
)
