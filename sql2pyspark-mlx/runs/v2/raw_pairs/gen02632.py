from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .filter(F.col("unit_price_range") >= 58)
    .orderBy(F.col("quantity_range").desc())
)
