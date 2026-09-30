from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= "2023-03-05") & (F.col("page") == "/checkout"))
)

agg = (
    base
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)

result = (
    agg
    .filter(F.col("pct_search") < 51)
    .select("page", "pct_search")
    .orderBy("page")
)
