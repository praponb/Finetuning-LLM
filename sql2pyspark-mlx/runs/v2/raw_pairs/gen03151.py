from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([2, 5, 8]))
    .groupBy("store_id")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") > 4)
)
