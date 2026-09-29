from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        F.date_trunc("month", "started_at").alias("activity_month"),
        "browser",
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
    .filter(F.col("firefox_rows") >= 1)
)
