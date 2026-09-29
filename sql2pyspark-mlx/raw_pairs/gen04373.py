from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("amount")
w2 = Window.partitionBy("method")
w3 = Window.orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_payments
    .withColumn("amount_dense_rank", F.dense_rank().over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("amount_avg_2", F.avg("amount").over(w3))
)
