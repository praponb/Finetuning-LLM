from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 370)
    .select(
        "id",
        "order_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("unit_price").desc())
)
