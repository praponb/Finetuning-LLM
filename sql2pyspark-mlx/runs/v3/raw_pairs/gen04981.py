from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") * 0.85), 2).alias("adjusted_unit_price"),
    )
)
