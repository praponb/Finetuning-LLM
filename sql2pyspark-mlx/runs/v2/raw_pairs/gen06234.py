from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 22) & (F.col("status").isin(["SHIPPED", "CANCELLED", "RETURNED"]) | (F.col("order_date") > "2025-04-01")))
    .withColumn("shipping_fee_adjusted", F.round((F.col("shipping_fee") * 0.9), 2))
    .withColumn("order_date_period", F.date_trunc("MONTH", "order_date"))
    .orderBy(F.col("shipping_fee").desc())
)
