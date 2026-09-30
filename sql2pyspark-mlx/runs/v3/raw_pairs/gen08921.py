from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .filter(F.col("quantity_count") < 5)
    .orderBy(F.col("total_unit_price").desc(), "product_id")
    .limit(5)
)
