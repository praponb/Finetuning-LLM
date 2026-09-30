from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("order_id").isin([10, 15, 26, 31]) & (F.col("quantity") >= 7))
)

summary = (
    base
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_order_items"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    summary
    .filter(F.col("avg_quantity") >= 3.2)
    .orderBy(F.col("avg_quantity").desc(), "product_id")
    .limit(5)
)
