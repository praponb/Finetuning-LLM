from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") < "2023-12-06")
    .groupBy("channel", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 2)
)
