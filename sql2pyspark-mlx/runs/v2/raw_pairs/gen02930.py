from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_payments
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "status", "amount", "method")
    .orderBy("status")
)
