from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_first())

result = (
    df_payments
    .withColumn("amount_avg_6", F.avg("amount").over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("amount_percent_rank", F.percent_rank().over(w3))
)
