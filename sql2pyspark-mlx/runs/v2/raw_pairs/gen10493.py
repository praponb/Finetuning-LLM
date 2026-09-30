from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2]))
    .select("store_id")
    .unionByName(df_inventory.filter(F.col("store_id").isin([2, 5])).select("store_id"))
)
