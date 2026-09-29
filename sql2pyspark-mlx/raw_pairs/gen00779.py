from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 34)
    .select("product_name")
    .orderBy(F.col("product_name").desc())
)
