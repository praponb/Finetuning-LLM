from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.max("rating").alias("max_rating"),
        F.round(((100 * F.sum(F.when(F.col("country") == "JP", 1).otherwise(0))) / F.count("*")), 1).alias("pct_jp"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("rating_count") >= 4)
    .drop("rating_count")
    .orderBy("country")
)
