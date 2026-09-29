from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-08-01")
    .groupBy("status")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("rnk").desc(), F.col("status").desc())
)
