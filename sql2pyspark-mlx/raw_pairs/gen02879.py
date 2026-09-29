from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNull() & F.array_contains("tags", "premium") & (F.col("cost") <= 430))
    .select("id", "product_name", "tags", "supplier_id")
)
