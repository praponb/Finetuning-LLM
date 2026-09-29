from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("duration_min").desc())

result = (
    df_sessions
    .groupBy(
        F.date_trunc("WEEK", "started_at").alias("week_start"),
        F.date_trunc("YEAR", "started_at").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.max("started_at").alias("last_seen"),
    )
    .withColumn("duration_min_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("us_count").asc_nulls_first())
)
