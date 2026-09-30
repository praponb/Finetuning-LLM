from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 370) | F.col("quantity").between(4, 6))
    .select(
        "product_id",
        "unit_price",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)
