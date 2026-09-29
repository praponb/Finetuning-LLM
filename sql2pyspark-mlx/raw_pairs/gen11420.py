from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("customer_id")

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy("total_amount")
)
