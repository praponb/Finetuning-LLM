from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-06-11", "2023-09-01") & F.col("store_id").isin([1, 2]))
)
