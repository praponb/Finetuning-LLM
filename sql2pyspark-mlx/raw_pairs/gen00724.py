from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("t.stark_count"),
        F.col("t.avg_stock_qty"),
    )
    .orderBy("avg_stock_qty", "id")
    .limit(5)
)
