from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 7)
    .orderBy("order_month", F.col("cnt").desc())
    .limit(5)
)
