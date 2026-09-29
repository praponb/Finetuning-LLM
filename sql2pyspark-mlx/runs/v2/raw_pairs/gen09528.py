from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.dense_rank().over(w))
)
