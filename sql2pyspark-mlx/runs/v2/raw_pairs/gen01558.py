from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.max("discount").alias("max_discount"))
)
