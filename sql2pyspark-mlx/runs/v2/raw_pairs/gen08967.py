from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
)
