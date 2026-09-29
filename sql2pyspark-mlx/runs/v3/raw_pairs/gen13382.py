from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
    .groupBy("order_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("amount_rank", F.dense_rank().over(w))
)
