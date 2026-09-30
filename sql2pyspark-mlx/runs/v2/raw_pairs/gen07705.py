from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "order_date", "customer_id", F.trim("status").alias("clean_status"))
)
