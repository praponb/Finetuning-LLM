from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "store")
    .withColumn("order_date_plus_11d", F.date_add("order_date", 11))
    .withColumn("clean_status", F.trim("status"))
    .orderBy("discount", F.col("order_date_plus_11d").desc(), "id")
    .limit(1)
)
