from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([5, 28]) | (F.col("amount") <= 630))
    .groupBy("channel")
    .agg(F.min("order_date").alias("first_order_date"))
)
