from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(120, 280) & F.col("product_id").isin([17, 19]))
    .select(
        "id",
        "unit_price",
        "product_id",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total_2"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(5)
)
