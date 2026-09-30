from pyspark.sql import functions as F

src = df_inventory.filter(F.col("last_restock") <= "2024-08-01")

summary = (
    src
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    summary
    .filter(F.col("min_quantity") < 220)
    .orderBy(F.col("avg_quantity").desc(), F.col("product_id").desc())
)
