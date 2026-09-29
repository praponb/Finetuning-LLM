from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.country"), F.col("t.row_count"))
    .orderBy("country")
)
