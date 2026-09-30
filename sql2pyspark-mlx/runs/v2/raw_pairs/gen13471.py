from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 5)
    .select(
        "order_id",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
    .orderBy(F.col("greatest_quantity_unit_price").desc())
)
