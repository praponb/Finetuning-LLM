from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(67, 116))
    .select("id", "product_id", "last_restock")
)
