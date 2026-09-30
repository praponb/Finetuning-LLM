from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull() & (F.col("supplier_id") == 26))
    .select("id", "product_name", "stock_qty")
    .orderBy(F.col("stock_qty").desc(), "id")
    .limit(3)
)
