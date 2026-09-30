from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") != "COMPLETED") & F.col("amount").between(1360, 1460))
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.count("discount").alias("discount_count"))
)
