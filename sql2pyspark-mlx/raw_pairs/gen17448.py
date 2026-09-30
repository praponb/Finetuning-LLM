from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2025-02-01")
    .select("id", "quantity", "last_restock", "store_id")
)
