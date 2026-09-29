from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("max_unit_price") < 380)
    .orderBy("quantity_range", "product_id")
    .limit(10)
)
