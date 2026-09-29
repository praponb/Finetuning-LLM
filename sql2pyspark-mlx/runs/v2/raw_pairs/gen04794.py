from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(~(F.col("store_id").isin([2, 5, 6]) & F.col("store_id").isin([3, 6])))
)

summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    summary
    .orderBy("avg_quantity", "product_id")
    .limit(10)
)
