from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") >= 1)
    .select("id", "supplier_id", "tags")
    .orderBy(F.col("supplier_id").desc(), "id")
)
