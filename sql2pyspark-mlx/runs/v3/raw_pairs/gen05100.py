from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("status") == "COMPLETED")

order_month_summary = (
    filtered_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "customer_id")
    .agg(F.sum("discount").alias("total_discount"))
)

result = order_month_summary.filter(F.col("total_discount") < 2.4)
