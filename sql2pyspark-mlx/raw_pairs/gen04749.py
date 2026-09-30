from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

latest = (
    df_sessions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .cube("device", "browser")
    .agg(
        F.max("started_at").alias("last_seen"),
        F.min("started_at").alias("first_seen"),
        F.max("started_at").alias("last_seen_2"),
        F.min("started_at").alias("first_seen_2"),
        F.count("*").alias("event_count"),
        F.max("started_at").alias("last_started_at"),
    )
    .orderBy("browser")
)
