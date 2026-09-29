from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 7)
    .groupBy("order_id", "product_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
    .filter(F.col("revenue") >= 45)
    .orderBy(F.col("revenue").desc())
)
