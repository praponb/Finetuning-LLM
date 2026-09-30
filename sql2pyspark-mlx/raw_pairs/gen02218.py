from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(150, 310) | (F.col("quantity") < 8))
    .select("product_id", (F.col("quantity") * F.col("unit_price")).alias("line_total"))
    .orderBy(F.col("product_id").desc())
)
