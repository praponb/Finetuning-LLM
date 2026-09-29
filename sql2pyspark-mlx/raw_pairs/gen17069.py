from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

result = (
    valid
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
    )
)
