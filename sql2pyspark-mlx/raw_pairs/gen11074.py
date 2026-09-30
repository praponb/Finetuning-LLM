from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 4]) & F.col("store_id").isin([4, 5]))
    .select("id", "last_restock", F.round("quantity", 0).alias("quantity_rounded"))
    .orderBy(F.col("last_restock").desc())
)
