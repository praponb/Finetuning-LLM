from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "order_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
)
