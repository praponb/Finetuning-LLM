from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(250, 360))
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
    .filter(F.col("revenue") >= 26)
    .select("product_id", "revenue")
    .orderBy(F.col("product_id").desc())
    .limit(1)
)
