from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity").between(3, 4) | (F.col("quantity") > 4)))
    .select(
        "id",
        "quantity",
        "product_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
