from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([8, 9]) | F.col("order_id").isin([13, 14]))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("id").desc())
)
