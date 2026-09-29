from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([25, 28, 30, 36]))
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
    .filter(F.col("pct_home") < 23)
    .select("page", "pct_home")
)
