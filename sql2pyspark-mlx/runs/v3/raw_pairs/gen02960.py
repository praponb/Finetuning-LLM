from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.dense_rank().over(w))
    .orderBy(F.col("paid_hour").desc())
    .limit(10)
)
