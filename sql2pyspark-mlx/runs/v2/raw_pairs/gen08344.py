from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 8)
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)
