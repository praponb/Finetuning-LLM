from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 88) & F.col("store_id").isin([1, 6]))
    .select("id", "product_id", F.round("quantity", 1).alias("quantity_rounded"))
)
