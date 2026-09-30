from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

result = (
    df_payments
    .filter(F.col("amount") <= 1420)
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy(F.col("amount").desc(), "paid_at")
)
