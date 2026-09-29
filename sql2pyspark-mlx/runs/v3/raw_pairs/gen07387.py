from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/search", "/cart"]))
    .groupBy("page")
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("unique_sessions") > 8)
    .select("page", "unique_sessions", "total_count")
)
