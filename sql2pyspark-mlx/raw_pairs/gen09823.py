from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_first(), "id")

latest = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"), "browser")
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count("*").alias("event_count"),
        F.min("started_at").alias("first_started_at"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.max("started_at").alias("last_started_at"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
