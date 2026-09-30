from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount")

result = (
    df_payments
    .select("id", "order_id", "amount", "status", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "order_id", "amount", "status")
    .orderBy(F.col("amount").desc(), F.col("order_id").desc())
)
