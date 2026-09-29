from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("customer_id").isin([18, 37, 38, 40]) & (F.col("channel") != "partner"))
)

summary = (
    valid_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.countDistinct("discount").alias("unique_customers"))
)

result = summary.select("order_month", "unique_customers")
