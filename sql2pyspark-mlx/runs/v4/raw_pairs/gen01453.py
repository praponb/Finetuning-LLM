from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 23) & (F.col("discount") > 0.18) & (F.col("customer_id") == 30))
    .withColumn("order_date_plus_months", F.add_months("order_date", 6))
    .withColumn("discount_rounded", F.round("discount", 1))
)
