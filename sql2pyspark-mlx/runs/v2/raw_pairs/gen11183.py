from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter((F.col("channel") == "mobile") | F.col("shipping_fee").between(25, 30))
    .withColumn("order_date_month_end", F.last_day("order_date"))
)

summary = (
    valid_orders
    .groupBy("status")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    summary
    .select("status", "amount_range")
    .orderBy(F.col("amount_range").desc())
)
