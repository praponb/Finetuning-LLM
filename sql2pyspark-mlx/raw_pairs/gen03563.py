from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 6, 18, 20]))
    .orderBy(F.col("order_id").desc(), "product_id")
)
