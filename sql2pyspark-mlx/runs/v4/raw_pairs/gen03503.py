from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 34)
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
        F.col("quantity").cast("string").alias("quantity_str"),
    )
    .orderBy(F.col("greatest_unit_price_quantity").desc(), F.col("id").desc())
)
