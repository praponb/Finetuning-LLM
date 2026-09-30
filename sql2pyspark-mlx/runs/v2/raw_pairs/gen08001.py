from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "store", "partner"]) & F.col("status").isin(["PENDING", "SHIPPED"]) & (F.col("amount") >= 770))
    .cube(F.month("order_date").alias("order_month"), "customer_id")
    .agg(F.countDistinct("shipping_fee").alias("unique_stores"))
    .orderBy("order_month", F.col("customer_id").desc())
)
