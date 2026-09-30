from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 8]))
    .select("product_id", F.round("quantity", 2).alias("quantity_rounded"))
)
