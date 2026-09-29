from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 7, 9, 10]) & F.col("store_id").isin([1, 2, 5]))
    .groupBy("store_id", "product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)
