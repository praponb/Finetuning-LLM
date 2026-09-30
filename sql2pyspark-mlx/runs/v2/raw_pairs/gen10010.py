from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.11, 0.13) & (F.col("store_id") == 13))
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "status")
    .agg((F.max("discount") - F.min("discount")).alias("discount_range"))
    .filter(F.col("discount_range") < 23)
)
