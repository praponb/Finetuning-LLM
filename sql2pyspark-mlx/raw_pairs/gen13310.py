from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")

result = (
    df_payments
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "status", "paid_at", "amount")
)
