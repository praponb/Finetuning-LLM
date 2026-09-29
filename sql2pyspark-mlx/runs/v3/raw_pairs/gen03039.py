from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-01-27")
    .select("last_restock", "product_id", "quantity")
    .orderBy("last_restock")
)
