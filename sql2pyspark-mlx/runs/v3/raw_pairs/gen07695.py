from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 200)
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
)
