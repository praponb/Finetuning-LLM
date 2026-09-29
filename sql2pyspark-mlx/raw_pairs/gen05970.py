from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .groupBy("segment", "country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
    )
    .filter(F.col("pct_new_york") < 46)
)
