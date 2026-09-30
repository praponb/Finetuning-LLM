from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "quantity",
        "product_id",
        F.round((F.col("unit_price") * 1.1), 2).alias("adjusted_unit_price"),
    )
)
