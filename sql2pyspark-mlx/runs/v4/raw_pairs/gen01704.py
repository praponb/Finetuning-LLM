from pyspark.sql import functions as F

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([3, 9, 11])), "left_anti")
)

result = (
    eligible
    .groupBy("browser", F.date_trunc("week", "started_at").alias("week_start"))
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .filter(F.col("tablet_count") > 1)
    .orderBy(F.col("safari_rows").desc())
)
