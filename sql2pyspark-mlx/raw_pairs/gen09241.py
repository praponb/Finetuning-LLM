from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 5, 8]))
    .select("id", "product_id", F.round("quantity", 2).alias("quantity_rounded"))
    .orderBy("id")
)
