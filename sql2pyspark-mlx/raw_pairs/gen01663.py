from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(F.countDistinct("amount").alias("unique_customers"))
    .orderBy("status")
)
