from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "customer_id", "status", F.ceil("discount").alias("discount_whole"))
)
