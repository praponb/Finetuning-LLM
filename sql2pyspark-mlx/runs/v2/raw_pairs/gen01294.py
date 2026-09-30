from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_customers"),
        F.avg("amount").alias("avg_amount"),
    )
)
