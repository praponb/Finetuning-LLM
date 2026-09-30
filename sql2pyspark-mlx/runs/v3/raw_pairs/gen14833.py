from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([3, 5, 9]))
    .select("quantity")
    .unionByName(df_inventory.filter(F.col("store_id").isin([2, 6])).select("quantity"))
)
