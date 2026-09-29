from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc())

result = (
    df_subscriptions
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 5)
    .orderBy("start_date")
)
