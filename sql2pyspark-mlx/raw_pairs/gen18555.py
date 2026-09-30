from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "device").orderBy(F.col("started_at").asc_nulls_first(), "id")

deduped = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("week", "started_at").alias("week_start"))
    .agg(
        F.max("started_at").alias("last_seen"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.min("started_at").alias("first_started_at"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("distinct_browsers").desc_nulls_first())
)
