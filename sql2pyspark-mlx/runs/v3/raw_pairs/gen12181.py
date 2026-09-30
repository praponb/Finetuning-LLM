from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull() | F.col("price").isNotNull())
    .select("id", "category", "supplier_id", "brand")
)
