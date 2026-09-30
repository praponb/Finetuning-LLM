from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("unique_orders").desc(), "order_id")
    .limit(3)
)
