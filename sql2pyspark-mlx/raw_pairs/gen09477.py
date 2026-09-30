from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 4) & F.col("product_id").isNotNull())
    .select(
        "id",
        "quantity",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("id").desc())
)
