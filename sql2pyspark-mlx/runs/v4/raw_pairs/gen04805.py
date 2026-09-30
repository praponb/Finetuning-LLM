from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").asc_nulls_last())

latest = (
    df_sessions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select("id", "device", "browser", "started_at", "country", "rank_in_group")
    .orderBy(F.col("rank_in_group").desc())
)
