from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost").isNotNull())
    .groupBy("supplier_id")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.rating"),
        F.col("t.min_stock_qty"),
        F.col("t.avg_cost"),
    )
    .orderBy(F.col("rating").desc())
)
