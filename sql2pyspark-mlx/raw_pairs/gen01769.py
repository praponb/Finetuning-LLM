from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.filter(F.col("order_date") > "2025-09-28").alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left_semi")
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.quantity"),
    )
    .orderBy(F.col("unit_price").desc(), F.col("quantity").desc())
)
