from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "TH"]) | F.col("country").isin(["JP", "US"]))
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("rating_bucket").desc(), F.col("stddev_rating").desc())
    .limit(20)
)
