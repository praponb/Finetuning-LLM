from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(233, 239))
    .select("id", "last_restock", "product_id")
)
