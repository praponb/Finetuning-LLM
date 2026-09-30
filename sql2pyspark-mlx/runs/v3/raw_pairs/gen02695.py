from pyspark.sql import functions as F

max_last_restock = df_inventory.filter(F.month("last_restock").isin([5, 8, 12])).agg(F.max("last_restock")).first()[0]

result = (
    df_inventory
    .filter(F.col("last_restock") == max_last_restock)
    .select("id", "last_restock")
)
