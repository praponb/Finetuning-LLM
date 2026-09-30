from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([3, 13, 32]) & F.col("unit_price").between(280, 390))
)

agg = (
    valid_order_items
    .groupBy("order_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    agg
    .select("order_id", "revenue")
    .orderBy(F.col("order_id").desc(), F.col("revenue").desc())
)
