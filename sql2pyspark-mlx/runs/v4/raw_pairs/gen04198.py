from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        F.date_trunc("WEEK", "started_at").alias("week_start"),
        "customer_id",
        F.date_trunc("YEAR", "started_at").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.min("started_at").alias("started_at"),
        F.min("started_at").alias("started_at_2"),
        F.max("started_at").alias("last_seen"),
    )
    .filter(F.col("num_countries") > 1)
    .orderBy(F.col("num_countries").desc())
)
