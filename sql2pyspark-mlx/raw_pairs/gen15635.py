from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "Bangkok"]), "tier_1").otherwise("other").alias("city_group"),
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("avg_rating") >= 3.7)
)
