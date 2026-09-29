from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~((F.col("order_date") >= "2024-05-01") & (F.col("discount") >= 0.11)))
    .groupBy("status")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("status", "total_amount")
)
