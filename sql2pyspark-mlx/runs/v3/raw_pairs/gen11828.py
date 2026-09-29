from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([7, 8, 9]) & (F.col("status") != "RETURNED"))
    .withColumn("clean_status", F.trim("status"))
    .orderBy("amount", F.col("customer_id").desc())
)
