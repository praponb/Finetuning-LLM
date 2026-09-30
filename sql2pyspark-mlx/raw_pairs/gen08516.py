from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 119)
    .select("id", "last_restock", "quantity", "product_id")
    .orderBy(F.col("quantity").desc())
)
