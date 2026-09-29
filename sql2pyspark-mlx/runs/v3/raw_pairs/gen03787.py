from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 7, 9]))
    .select("store_id")
    .unionByName(df_inventory.filter(F.col("quantity") <= 177).select("store_id"))
)
