from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy(F.col("total_unit_price").desc())
)
