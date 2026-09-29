from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "city",
        "country",
    )
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
    .filter(F.col("us_count") < 2)
    .orderBy("country")
)
