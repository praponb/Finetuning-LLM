from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["SG", "DE"]))
    .groupBy("country", "segment")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") < 6)
    .select("country", "segment", "pct_london", "total_count")
)
