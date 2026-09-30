from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNull())
    .withColumn("rating_whole", F.floor("rating"))
)

city_summary = (
    base
    .groupBy("city", "country")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating_whole")).otherwise(0)).alias("tokyo_rating_whole"),
    )
)

result = city_summary.filter(F.col("tokyo_rating_whole") >= 67)
