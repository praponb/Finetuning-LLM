from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 33)
    .groupBy("product_id", "order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .orderBy(F.col("product_id").desc(), F.col("revenue").desc(), "order_id")
    .limit(10)
)
