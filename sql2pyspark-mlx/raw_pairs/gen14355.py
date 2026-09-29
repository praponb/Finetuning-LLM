from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() | (F.col("customer_id") == 14))
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.min("order_date").alias("first_order_date"))
)
