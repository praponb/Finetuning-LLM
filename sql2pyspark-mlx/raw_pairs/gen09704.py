from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("quantity_count").desc(), F.col("product_id").desc(), "order_id")
    .limit(50)
)
