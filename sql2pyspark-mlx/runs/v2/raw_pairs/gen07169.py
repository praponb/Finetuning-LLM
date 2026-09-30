from pyspark.sql import functions as F

avg_unit_price = df_order_items.agg(F.avg("unit_price")).first()[0]

result = (
    df_order_items
    .filter(F.col("unit_price") >= avg_unit_price)
    .select("id", "unit_price", "product_id", "order_id")
    .orderBy("product_id", F.col("id").desc())
    .limit(10)
)
