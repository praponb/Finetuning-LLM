from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 7))
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy("quantity_range", F.col("unit_price_range").desc(), "product_id")
    .limit(10)
)
