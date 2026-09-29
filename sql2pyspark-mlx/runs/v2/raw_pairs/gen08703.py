from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_orders
    .filter(F.col("discount") <= 0.15)
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.rank().over(w))
    .orderBy("order_month")
)
