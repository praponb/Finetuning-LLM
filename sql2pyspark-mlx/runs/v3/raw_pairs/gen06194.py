from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        F.round((F.col("unit_price") * 0.85), 2).alias("adjusted_unit_price"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("quantity").desc(), "adjusted_unit_price")
)
