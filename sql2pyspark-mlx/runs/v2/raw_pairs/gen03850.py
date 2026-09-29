from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([5, 20]) & F.col("quantity").isNotNull())
    .select(
        "id",
        "product_id",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)
