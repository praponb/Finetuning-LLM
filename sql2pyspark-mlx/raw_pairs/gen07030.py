from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "browser").orderBy(F.col("started_at").desc_nulls_first(), "id")

deduped = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "started_at")
)

result = (
    deduped
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.max("started_at").alias("ended_at"),
        F.min("started_at").alias("first_started_at"),
        F.max("started_at").alias("last_started_at"),
        F.min("started_at").alias("started_at"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("week_start")
)
