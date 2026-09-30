from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date")

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-02-01", "2024-06-08"))
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "status", "start_date", "plan")
    .orderBy(F.col("id").desc())
)
