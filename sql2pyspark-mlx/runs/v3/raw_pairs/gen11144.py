from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(153, 164) & F.col("store_id").isin([2, 3]))
    .select("store_id", "last_restock")
)
