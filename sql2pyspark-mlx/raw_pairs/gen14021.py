from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "browser", "started_at", "device")
    .orderBy("id")
)
