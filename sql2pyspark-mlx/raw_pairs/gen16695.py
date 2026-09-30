from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 135)
    .select("quantity", "last_restock", "product_id")
)
