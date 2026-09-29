from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([21, 31, 40]))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .filter(F.col("min_quantity") < 3.1)
    .select("product_id", "min_quantity", "unit_price_range")
    .orderBy(F.col("product_id").desc())
)
