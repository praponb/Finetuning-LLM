from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["DE", "SG"]), "priority").otherwise("other").alias("country_group"),
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("us_count") >= 3)
    .orderBy(F.col("rating_bucket").desc(), "country_group")
    .limit(5)
)
