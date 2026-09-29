from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec_whole").desc_nulls_first())

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-12-17"))) & F.col("page").isNotNull())
    .select("*", F.ceil("duration_sec").alias("duration_sec_whole"))
)

result = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
