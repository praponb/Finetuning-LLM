from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2024-06-30") & F.col("store_id").isin([1, 6]))
    .select("id", "quantity")
    .orderBy(F.col("quantity").desc(), "id")
)
