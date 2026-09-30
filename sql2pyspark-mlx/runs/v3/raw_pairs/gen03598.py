from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("supplier_id") == 24)
    .groupBy("supplier_id")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "left")
    .filter(F.col("P.stock_qty_count") > 5)
    .select(F.col("s.rating"), F.col("P.stock_qty_count"))
    .orderBy(F.col("rating").desc())
)
