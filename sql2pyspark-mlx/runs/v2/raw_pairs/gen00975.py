from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([11, 40]) & ((F.col("stock_qty") <= 281) | (F.col("cost") < 310)))
    .select("id", "category", "product_name")
    .orderBy("product_name")
)
