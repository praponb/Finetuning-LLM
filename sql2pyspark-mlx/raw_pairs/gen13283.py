from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5, 6]) | F.col("last_restock").between("2023-06-07", "2023-07-26"))
    .select("store_id")
)
