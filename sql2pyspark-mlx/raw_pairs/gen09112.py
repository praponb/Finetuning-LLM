from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.12, 0.16) & F.col("customer_id").isNotNull())
    .select("id", "channel", F.dayofmonth("order_date").alias("order_date_day"))
)
