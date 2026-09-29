from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee")

result = (
    df_subscriptions
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "status", "monthly_fee", "end_date")
    .orderBy(F.col("end_date").desc(), F.col("status").desc())
)
