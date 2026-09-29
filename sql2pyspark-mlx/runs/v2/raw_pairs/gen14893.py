from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.count("*").alias("row_count"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
    )
    .filter(F.col("corporate_count") > 1)
    .drop("corporate_count")
    .orderBy(F.col("pct_tokyo").desc(), "segment")
    .limit(5)
)
