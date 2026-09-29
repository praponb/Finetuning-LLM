from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("rating").alias("unique_citys"),
        F.sum(F.when(F.col("country") == "SG", F.col("rating")).otherwise(0)).alias("sg_rating"),
    )
    .filter(F.col("sg_rating") < 42)
    .drop("sg_rating")
)
