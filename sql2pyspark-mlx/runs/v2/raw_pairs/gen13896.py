from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 5)
    .orderBy("country", "customer_id")
)
