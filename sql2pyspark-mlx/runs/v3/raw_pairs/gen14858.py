from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .withColumn("order_date_dayofweek", F.dayofweek("order_date"))
)
