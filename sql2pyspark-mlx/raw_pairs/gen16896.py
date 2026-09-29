from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("customer_id") == 11)
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "device", "started_at")
    .orderBy("started_at")
)
