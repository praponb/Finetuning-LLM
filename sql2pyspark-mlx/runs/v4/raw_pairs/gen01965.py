from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 240)
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .filter(F.col("total_quantity") < 38)
    .orderBy(F.col("product_id").desc())
    .limit(5)
)
