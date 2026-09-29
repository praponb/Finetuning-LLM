from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 86) & ((F.year("last_restock") == 2023) | F.col("store_id").isin([4, 5])))
    .select("id", "product_id")
    .orderBy("product_id", "id")
    .limit(10)
)
