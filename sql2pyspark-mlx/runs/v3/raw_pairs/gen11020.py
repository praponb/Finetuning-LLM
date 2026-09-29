from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("order_date").alias("event_date"))
    .unionByName(df_customers.select(F.col("signup_date").alias("event_date")))
)
