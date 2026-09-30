from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 3)
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        F.round("unit_price", 1).alias("unit_price_rounded"),
    )
    .orderBy(F.col("quantity").desc())
)
