from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city", "country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
        F.countDistinct("rating").alias("unique_citys"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 5)
    .drop("total_count")
)
