from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.min("discount").alias("min_discount"))
    .filter(F.col("min_discount") < 0.19)
)
