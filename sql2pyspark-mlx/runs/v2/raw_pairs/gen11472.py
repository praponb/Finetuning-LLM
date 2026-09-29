from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("product_id") == 8).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(F.array_contains(F.col("p.tags"), "premium"))
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.stock_qty"))
    .orderBy(F.col("id").desc())
)
