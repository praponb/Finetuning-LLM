from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("quantity") >= 3)

agg = (
    filtered_order_items
    .groupBy("order_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = agg.select("order_id", "revenue")
