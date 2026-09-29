from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("unit_price").desc())
)
