from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("order_date")
    .agg(F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("total_returned"))
)
