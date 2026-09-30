from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 5]) & (F.col("product_id") == 2) & F.col("quantity").isNotNull())
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
