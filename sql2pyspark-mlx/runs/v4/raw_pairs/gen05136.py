from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 7))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("min_quantity") > 6.5)
    .select("product_id", "min_quantity", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc())
)
