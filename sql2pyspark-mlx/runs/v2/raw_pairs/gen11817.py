from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 9)
    .select("last_restock", "product_id")
    .orderBy("last_restock")
)
