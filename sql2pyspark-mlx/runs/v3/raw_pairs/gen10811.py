from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.min("rating").alias("min_rating"),
    )
    .orderBy("country")
)
