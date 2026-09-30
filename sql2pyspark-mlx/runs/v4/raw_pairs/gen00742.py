from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "method", "paid_at", "status")
)
