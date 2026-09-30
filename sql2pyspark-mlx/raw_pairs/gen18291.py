from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 162) & (F.col("quantity") <= 84) & F.col("product_id").isin([1, 3, 7, 8]))
    .select("id", "last_restock", F.round("quantity", 1).alias("quantity_rounded"))
)
