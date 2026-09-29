from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .filter(F.col("pct_search") < 78)
    .select("page", "pct_search")
    .orderBy(F.col("page").desc())
    .limit(10)
)
