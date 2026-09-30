from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 5, 10, 13]) | F.col("order_id").isin([5, 11, 16]))
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)
