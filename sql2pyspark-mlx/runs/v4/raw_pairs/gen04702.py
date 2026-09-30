from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2025-02-01") & F.month("last_restock").isin([3, 9, 11]))
    .select("id", "quantity", "last_restock", "product_id")
    .orderBy(F.col("product_id").desc(), "id")
    .limit(50)
)
