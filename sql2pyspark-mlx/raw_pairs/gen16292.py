from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.min("amount").alias("min_amount"))
)
