from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter((F.col("unit_price") < 370) & (F.col("quantity") < 7) & F.col("quantity").between(3, 5))
)

agg = (
    recent_order_items
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    agg
    .filter(F.col("unit_price_range") > 75)
    .select("product_id", "unit_price_count", "unit_price_range")
)
