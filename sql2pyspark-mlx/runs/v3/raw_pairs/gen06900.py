from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(230, 280) & F.col("quantity").between(5, 8))
    .select(
        "id",
        "product_id",
        F.when(F.col("unit_price") >= 380, "High").when(F.col("unit_price") >= 120, "Medium").otherwise("Low").alias("unit_price_tier"),
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("line_total").desc())
)
