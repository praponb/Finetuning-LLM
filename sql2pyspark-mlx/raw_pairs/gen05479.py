from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([11, 14, 28]))
    .select(F.col("opened_date").alias("event_date"))
    .subtract(df_orders.select(F.col("order_date").alias("event_date")))
)
