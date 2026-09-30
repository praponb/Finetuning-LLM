from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy("rating_bucket", "country", "city")
    .limit(5)
)
