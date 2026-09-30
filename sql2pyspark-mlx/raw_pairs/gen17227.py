from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 5)
    .withColumn("order_date_period", F.date_trunc("year", "order_date"))
    .withColumn("amount_int", F.col("amount").cast("int"))
)
