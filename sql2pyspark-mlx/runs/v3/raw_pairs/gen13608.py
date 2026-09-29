from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-08-01")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "order_id", "amount", "method")
)
