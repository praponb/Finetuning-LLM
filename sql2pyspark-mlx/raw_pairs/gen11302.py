from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 4) & F.col("quantity").between(69, 213))
    .select("id", "quantity", "last_restock", "store_id")
)
