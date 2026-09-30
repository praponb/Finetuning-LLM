from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 210) & F.col("order_id").isin([15, 23, 27, 36]))
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("quantity_count").desc())
)
