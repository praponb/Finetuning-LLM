from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.countDistinct("discount").alias("unique_customers"),
        F.min("order_date").alias("min_order_date"),
    )
    .filter(F.col("unique_customers") >= 5)
    .orderBy("min_order_date")
)
