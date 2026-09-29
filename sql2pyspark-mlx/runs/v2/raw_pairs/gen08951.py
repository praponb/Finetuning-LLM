from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2025-01-01") & (F.col("product_id") == 5))
)
