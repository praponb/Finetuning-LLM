from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("paid_at").desc())
)
