from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "order_id",
        "quantity",
        F.round((F.col("unit_price") * 1.07), 2).alias("adjusted_unit_price"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
