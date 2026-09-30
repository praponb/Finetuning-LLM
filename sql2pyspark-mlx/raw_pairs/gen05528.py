from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 1.8)
    .groupBy(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("rating_count") > 4)
)
