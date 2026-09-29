from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "device", "started_at")
    .orderBy(F.col("started_at").desc())
)
