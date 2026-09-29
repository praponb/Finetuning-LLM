from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("pct_chiang_mai") >= 62)
    .select("segment", "pct_chiang_mai")
)
