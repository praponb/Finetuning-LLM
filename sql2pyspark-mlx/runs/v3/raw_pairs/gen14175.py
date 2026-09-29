from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 7, 10]))
    .select("quantity", "product_id", F.round("quantity", 1).alias("quantity_rounded"))
    .orderBy(F.col("product_id").desc(), "quantity")
)
