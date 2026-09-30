from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at")

result = (
    df_sessions
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "customer_id", "started_at")
)
