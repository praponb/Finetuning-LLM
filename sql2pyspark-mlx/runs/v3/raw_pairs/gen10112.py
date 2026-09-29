from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-06-23")
    .select("last_restock")
)
