from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.round((F.col("unit_price") * 0.9), 2).alias("adjusted_unit_price"),
    )
)
