from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 6) & F.col("order_id").isin([5, 17, 22]) & F.col("unit_price").between(220, 240))
    .select("id", "order_id", F.round("unit_price", 2).alias("unit_price_rounded"))
    .orderBy(F.col("order_id").desc(), F.col("id").desc())
)
