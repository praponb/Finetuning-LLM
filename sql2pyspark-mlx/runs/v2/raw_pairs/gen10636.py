from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.col("last_restock") >= "2024-10-25") & ((F.col("last_restock") > "2024-09-01") | F.col("store_id").isin([2, 3])))
)

summary = (
    recent_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    summary
    .select("product_id", "total_quantity", "avg_quantity")
    .orderBy("total_quantity")
)
