from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") != "SHIPPED") & (F.col("channel") != "store") & (F.col("order_date") >= "2025-02-01"))
    .withColumn("order_date_month_end", F.last_day("order_date"))
    .orderBy("channel")
)
