from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("avg_rating") > 52)
    .select("country", "rating_bucket", "avg_rating", "city_list")
    .orderBy("avg_rating")
)
