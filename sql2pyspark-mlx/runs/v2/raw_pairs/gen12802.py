from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id", "channel")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 4000)
    .orderBy("customer_id")
)
