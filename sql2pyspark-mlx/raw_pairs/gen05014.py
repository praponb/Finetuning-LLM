from pyspark.sql import functions as F

min_last_restock = df_inventory.filter(F.col("quantity") < 181).agg(F.min("last_restock")).first()[0]

result = (
    df_inventory
    .filter(F.col("last_restock") == min_last_restock)
    .select("id", "last_restock", "quantity")
    .orderBy(F.col("id").desc())
)
