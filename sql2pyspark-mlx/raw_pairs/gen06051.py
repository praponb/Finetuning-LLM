from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 310, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
)
