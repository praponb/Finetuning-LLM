from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 7)
    .select(
        "order_id",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
    .orderBy(F.col("greatest_unit_price_quantity").desc())
)
