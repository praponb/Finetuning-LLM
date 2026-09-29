from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 8600)
)
