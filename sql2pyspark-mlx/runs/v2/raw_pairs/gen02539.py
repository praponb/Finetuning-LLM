from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 3) & F.col("store_id").isin([3, 5, 6]))
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
