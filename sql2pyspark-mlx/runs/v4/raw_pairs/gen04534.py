from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("browser", "country")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count("*").alias("event_count"),
        F.max("started_at").alias("ended_at"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
