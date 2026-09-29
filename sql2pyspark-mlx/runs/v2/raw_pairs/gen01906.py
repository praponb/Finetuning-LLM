from pyspark.sql import functions as F

result = (
    df_products
    .select("product_name", "stock_qty")
    .orderBy(F.col("stock_qty").desc())
)
