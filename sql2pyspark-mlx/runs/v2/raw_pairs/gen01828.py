from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_month")

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
)
