from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("customer_id").isin([1, 38]))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "customer_id", "started_at")
    .orderBy(F.col("id").desc())
)
