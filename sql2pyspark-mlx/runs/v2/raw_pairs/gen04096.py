from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(F.countDistinct("discount").alias("unique_customers"))
)
