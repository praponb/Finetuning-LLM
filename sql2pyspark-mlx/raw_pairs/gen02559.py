from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

deduped = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .rollup("activity_month", "country")
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.min("started_at").alias("first_seen"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.min("started_at").alias("started_at"),
        F.max("started_at").alias("last_seen"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .orderBy("de_events")
)
