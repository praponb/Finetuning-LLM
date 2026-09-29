from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at")

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("amount").desc())
)
