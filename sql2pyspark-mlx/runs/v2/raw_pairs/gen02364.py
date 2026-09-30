from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([6, 28, 38, 40]) & ((F.col("product_id") == 8) | F.col("order_id").isin([29, 38, 39])))
    .groupBy("product_id", "order_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("min_quantity") < 8.0)
    .orderBy(F.col("product_id").desc())
)
