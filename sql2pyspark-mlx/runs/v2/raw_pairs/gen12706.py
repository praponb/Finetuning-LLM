from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") != "store")
    .groupBy("customer_id")
    .agg(F.countDistinct("amount").alias("unique_customers"))
    .filter(F.col("unique_customers") >= 4)
)
