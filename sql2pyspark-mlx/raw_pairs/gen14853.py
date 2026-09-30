from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([18, 28, 30, 33]))
    .groupBy("order_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
    .orderBy("revenue", "order_id")
)
