from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at_day").desc())

base = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .withColumn("started_at_day", F.col("started_at").cast("date"))
)

result = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "device", "browser", "started_at", "started_at_day")
)
