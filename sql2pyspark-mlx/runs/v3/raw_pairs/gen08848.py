from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("amount_rank", F.dense_rank().over(w))
    .orderBy("total_amount")
)
