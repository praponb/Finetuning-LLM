from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 250) & F.col("unit_price").between(170, 300))
    .select(
        "id",
        "quantity",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
    .orderBy(F.col("id").desc())
)
