from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 7)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.round("unit_price", 1).alias("unit_price_rounded"),
    )
    .orderBy("id", F.col("unit_price").desc())
)
