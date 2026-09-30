from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isin([3, 12, 15, 23]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

result = (
    valid
    .select("product_id", "id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isin([7, 10, 26]))
        .select("product_id", "id")
    )
)
