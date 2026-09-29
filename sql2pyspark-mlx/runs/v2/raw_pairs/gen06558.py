from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/checkout", "/home"]) & (F.col("duration_sec") <= 211))
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
    )
)
