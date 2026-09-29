from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty").between(162, 390))
    .groupBy("supplier_id")
    .agg(F.min("price").alias("min_price"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("t.min_price"))
    .orderBy("country")
)
