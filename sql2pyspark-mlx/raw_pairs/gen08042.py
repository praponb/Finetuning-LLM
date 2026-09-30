from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        F.when(F.col("country").isin(["TH", "JP"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy("rating_bucket")
)
