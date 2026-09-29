from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "product_id",
        "order_id",
        "quantity",
        F.round((F.col("quantity") * 1.15), 2).alias("adjusted_quantity"),
    )
)
